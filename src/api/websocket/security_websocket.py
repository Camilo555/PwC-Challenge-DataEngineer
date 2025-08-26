"""
WebSocket Security Handler for Real-time Security Dashboard
Provides secure WebSocket connections with authentication, authorization,
real-time security event streaming, and threat monitoring.
"""
import asyncio
import json
import uuid
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Union
from dataclasses import dataclass, field

from fastapi import WebSocket, WebSocketDisconnect, status
from fastapi.websockets import WebSocketState
from starlette.websockets import WebSocketDisconnect as StarletteWebSocketDisconnect

from core.logging import get_logger
from core.security.enterprise_security_orchestrator import get_security_orchestrator
from core.security.advanced_security import get_security_manager, SecurityEventType, ThreatLevel
from core.security.enterprise_dlp import EnterpriseDLPManager
from core.security.compliance_framework import get_compliance_engine
from core.security.enhanced_access_control import get_access_control_manager
from api.v1.services.enhanced_auth_service import get_auth_service, verify_jwt_token


logger = get_logger(__name__)


class WebSocketEventType(Enum):
    """WebSocket event types for security dashboard"""
    SECURITY_ALERT = "security_alert"
    DLP_INCIDENT = "dlp_incident"
    COMPLIANCE_VIOLATION = "compliance_violation"
    ACCESS_DENIED = "access_denied"
    AUTHENTICATION_EVENT = "authentication_event"
    SYSTEM_HEALTH = "system_health"
    THREAT_DETECTION = "threat_detection"
    REAL_TIME_METRICS = "real_time_metrics"
    USER_ACTIVITY = "user_activity"
    DASHBOARD_UPDATE = "dashboard_update"


class SecurityClearanceLevel(Enum):
    """Security clearance levels for WebSocket access"""
    PUBLIC = 0
    INTERNAL = 1
    CONFIDENTIAL = 2
    SECRET = 3
    TOP_SECRET = 4


@dataclass
class WebSocketConnection:
    """WebSocket connection metadata"""
    connection_id: str
    websocket: WebSocket
    user_id: str
    session_id: Optional[str]
    permissions: List[str]
    clearance_level: int
    ip_address: str
    user_agent: str
    connected_at: datetime
    last_activity: datetime
    subscription_channels: Set[str] = field(default_factory=set)
    security_context: Dict[str, Any] = field(default_factory=dict)
    rate_limit_count: int = 0
    rate_limit_reset: datetime = field(default_factory=datetime.now)


@dataclass
class SecurityWebSocketMessage:
    """Security WebSocket message format"""
    event_type: WebSocketEventType
    timestamp: datetime
    data: Dict[str, Any]
    security_classification: str = "internal"
    requires_permissions: List[str] = field(default_factory=list)
    redaction_level: str = "standard"
    metadata: Dict[str, Any] = field(default_factory=dict)


class SecurityWebSocketManager:
    """Manager for secure WebSocket connections and real-time security streaming"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.connections: Dict[str, WebSocketConnection] = {}
        self.channel_subscriptions: Dict[str, Set[str]] = {}  # channel -> connection_ids
        self.connection_lock = asyncio.Lock()
        
        # Component integrations
        self.auth_service = get_auth_service()
        self.security_manager = get_security_manager()
        self.security_orchestrator = get_security_orchestrator()
        self.dlp_manager = EnterpriseDLPManager()
        self.compliance_engine = get_compliance_engine()
        self.access_manager = get_access_control_manager()
        
        # Rate limiting configuration
        self.rate_limit_per_minute = 60
        self.max_connections_per_user = 5
        
        # Available channels and their required permissions
        self.security_channels = {
            'security_alerts': ['perm_security_read', 'perm_admin_system'],
            'dlp_incidents': ['perm_dlp_read', 'perm_security_read'],
            'compliance_violations': ['perm_compliance_read', 'perm_audit_logs'],
            'access_control_events': ['perm_access_control_read', 'perm_audit_logs'],
            'authentication_events': ['perm_auth_logs', 'perm_security_read'],
            'system_health': ['perm_system_monitor', 'perm_admin_system'],
            'threat_intelligence': ['perm_threat_intel', 'perm_security_read'],
            'real_time_metrics': ['perm_metrics_read', 'perm_admin_system'],
            'audit_events': ['perm_audit_logs', 'perm_compliance_read'],
            'dashboard_updates': ['perm_dashboard_read']
        }
        
        # Start background tasks
        asyncio.create_task(self._periodic_health_check())
        asyncio.create_task(self._periodic_metrics_broadcast())
        
        self.logger.info("Security WebSocket Manager initialized")
    
    async def connect_websocket(
        self,
        websocket: WebSocket,
        token: str,
        requested_channels: List[str] = None,
        client_info: Dict[str, str] = None
    ) -> Optional[str]:
        """
        Establish secure WebSocket connection with authentication and authorization
        
        Args:
            websocket: WebSocket connection
            token: JWT authentication token
            requested_channels: List of channels to subscribe to
            client_info: Additional client information
            
        Returns:
            Connection ID if successful, None if failed
        """
        
        try:
            # Extract client information
            client_ip = self._get_client_ip(websocket)
            client_info = client_info or {}
            user_agent = client_info.get('user_agent', 'Unknown')
            
            self.logger.info(f"WebSocket connection attempt from {client_ip}")
            
            # Authenticate the token
            auth_result = await self.auth_service.authenticate_jwt(token, client_ip)
            
            if not auth_result.success:
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Authentication failed")
                self.logger.warning(f"WebSocket authentication failed for {client_ip}: {auth_result.error_message}")
                return None
            
            user_id = auth_result.user_id
            permissions = auth_result.permissions
            clearance_level = getattr(auth_result.token_claims, 'clearance_level', 0)
            session_id = getattr(auth_result.token_claims, 'session_id', None)
            
            # Check if user has minimum required permissions
            if not any(perm in permissions for perm in ['perm_dashboard_read', 'perm_security_read', 'perm_admin_system']):
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Insufficient permissions")
                self.logger.warning(f"WebSocket access denied for user {user_id}: insufficient permissions")
                return None
            
            # Check connection limits per user
            user_connections = [
                conn for conn in self.connections.values() 
                if conn.user_id == user_id
            ]
            
            if len(user_connections) >= self.max_connections_per_user:
                await websocket.close(code=status.WS_1013_TRY_AGAIN_LATER, reason="Too many connections")
                self.logger.warning(f"Connection limit exceeded for user {user_id}")
                return None
            
            # Accept the WebSocket connection
            await websocket.accept()
            
            # Create connection record
            connection_id = str(uuid.uuid4())
            connection = WebSocketConnection(
                connection_id=connection_id,
                websocket=websocket,
                user_id=user_id,
                session_id=session_id,
                permissions=permissions,
                clearance_level=clearance_level,
                ip_address=client_ip,
                user_agent=user_agent,
                connected_at=datetime.now(),
                last_activity=datetime.now(),
                security_context={
                    'risk_score': auth_result.risk_score,
                    'authentication_method': getattr(auth_result.token_claims, 'authentication_method', 'jwt'),
                    'mfa_verified': getattr(auth_result.token_claims, 'mfa_verified', False)
                }
            )
            
            # Store connection
            async with self.connection_lock:
                self.connections[connection_id] = connection
            
            # Subscribe to requested channels
            if requested_channels:
                await self._subscribe_to_channels(connection_id, requested_channels)
            else:
                # Default channels based on permissions
                default_channels = self._get_default_channels(permissions)
                await self._subscribe_to_channels(connection_id, default_channels)
            
            # Send connection confirmation
            await self._send_to_connection(
                connection_id,
                SecurityWebSocketMessage(
                    event_type=WebSocketEventType.DASHBOARD_UPDATE,
                    timestamp=datetime.now(),
                    data={
                        'type': 'connection_established',
                        'connection_id': connection_id,
                        'subscribed_channels': list(connection.subscription_channels),
                        'user_permissions': permissions,
                        'clearance_level': clearance_level
                    }
                )
            )
            
            # Log successful connection
            await self._log_websocket_event(
                connection_id,
                'websocket_connected',
                {
                    'user_id': user_id,
                    'ip_address': client_ip,
                    'channels': list(connection.subscription_channels)
                }
            )
            
            self.logger.info(f"WebSocket connected: {connection_id} for user {user_id}")
            
            return connection_id
            
        except Exception as e:
            self.logger.error(f"WebSocket connection failed: {e}")
            try:
                await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="Internal server error")
            except:
                pass
            return None
    
    async def disconnect_websocket(self, connection_id: str, reason: str = "Client disconnect"):
        """Safely disconnect and cleanup WebSocket connection"""
        
        try:
            async with self.connection_lock:
                connection = self.connections.get(connection_id)
                if not connection:
                    return
                
                # Remove from channel subscriptions
                for channel in list(connection.subscription_channels):
                    await self._unsubscribe_from_channel(connection_id, channel)
                
                # Close WebSocket if still connected
                if connection.websocket.client_state == WebSocketState.CONNECTED:
                    await connection.websocket.close(reason=reason)
                
                # Log disconnection
                await self._log_websocket_event(
                    connection_id,
                    'websocket_disconnected',
                    {
                        'user_id': connection.user_id,
                        'reason': reason,
                        'duration_seconds': (datetime.now() - connection.connected_at).total_seconds()
                    }
                )
                
                # Remove connection
                del self.connections[connection_id]
                
                self.logger.info(f"WebSocket disconnected: {connection_id}, reason: {reason}")
                
        except Exception as e:
            self.logger.error(f"Error disconnecting WebSocket {connection_id}: {e}")
    
    async def handle_websocket_message(self, connection_id: str, message: str):
        """Handle incoming WebSocket message from client"""
        
        try:
            connection = self.connections.get(connection_id)
            if not connection:
                return
            
            # Update last activity
            connection.last_activity = datetime.now()
            
            # Rate limiting check
            if not await self._check_rate_limit(connection_id):
                await self._send_error(connection_id, "Rate limit exceeded")
                return
            
            # Parse message
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                await self._send_error(connection_id, "Invalid JSON format")
                return
            
            message_type = data.get('type')
            
            if message_type == 'subscribe':
                channels = data.get('channels', [])
                await self._subscribe_to_channels(connection_id, channels)
                
            elif message_type == 'unsubscribe':
                channels = data.get('channels', [])
                for channel in channels:
                    await self._unsubscribe_from_channel(connection_id, channel)
                    
            elif message_type == 'get_dashboard_data':
                await self._send_dashboard_data(connection_id)
                
            elif message_type == 'get_real_time_metrics':
                await self._send_real_time_metrics(connection_id)
                
            elif message_type == 'security_command':
                await self._handle_security_command(connection_id, data.get('command', {}))
                
            else:
                await self._send_error(connection_id, f"Unknown message type: {message_type}")
            
            # Log message handling
            await self._log_websocket_event(
                connection_id,
                'message_received',
                {
                    'message_type': message_type,
                    'user_id': connection.user_id
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error handling WebSocket message: {e}")
            await self._send_error(connection_id, "Message processing error")
    
    async def broadcast_security_event(
        self,
        event_type: WebSocketEventType,
        data: Dict[str, Any],
        channel: str = None,
        required_permissions: List[str] = None,
        security_classification: str = "internal"
    ):
        """Broadcast security event to appropriate connections"""
        
        try:
            # Create message
            message = SecurityWebSocketMessage(
                event_type=event_type,
                timestamp=datetime.now(),
                data=data,
                security_classification=security_classification,
                requires_permissions=required_permissions or [],
                redaction_level=self._determine_redaction_level(security_classification)
            )
            
            # Determine target connections
            target_connections = []
            
            if channel:
                # Send to specific channel subscribers
                connection_ids = self.channel_subscriptions.get(channel, set())
                target_connections = [
                    self.connections[conn_id] 
                    for conn_id in connection_ids 
                    if conn_id in self.connections
                ]
            else:
                # Send to all appropriate connections
                target_connections = list(self.connections.values())
            
            # Filter connections based on permissions and clearance
            authorized_connections = []
            for connection in target_connections:
                if await self._is_authorized_for_message(connection, message):
                    authorized_connections.append(connection)
            
            # Send to authorized connections
            for connection in authorized_connections:
                await self._send_to_connection(connection.connection_id, message)
            
            self.logger.info(f"Broadcast {event_type.value} to {len(authorized_connections)} connections")
            
        except Exception as e:
            self.logger.error(f"Error broadcasting security event: {e}")
    
    async def send_real_time_alert(
        self,
        alert_type: str,
        severity: str,
        title: str,
        description: str,
        metadata: Dict[str, Any] = None
    ):
        """Send real-time security alert to dashboard"""
        
        alert_data = {
            'alert_id': str(uuid.uuid4()),
            'alert_type': alert_type,
            'severity': severity,
            'title': title,
            'description': description,
            'timestamp': datetime.now().isoformat(),
            'metadata': metadata or {}
        }
        
        await self.broadcast_security_event(
            event_type=WebSocketEventType.SECURITY_ALERT,
            data=alert_data,
            channel='security_alerts',
            required_permissions=['perm_security_read'],
            security_classification='confidential'
        )
    
    # Private helper methods
    
    async def _subscribe_to_channels(self, connection_id: str, channels: List[str]):
        """Subscribe connection to security channels"""
        
        connection = self.connections.get(connection_id)
        if not connection:
            return
        
        successful_subscriptions = []
        failed_subscriptions = []
        
        for channel in channels:
            # Check if channel exists
            if channel not in self.security_channels:
                failed_subscriptions.append({'channel': channel, 'reason': 'Channel does not exist'})
                continue
            
            # Check permissions
            required_permissions = self.security_channels[channel]
            if not any(perm in connection.permissions for perm in required_permissions):
                failed_subscriptions.append({'channel': channel, 'reason': 'Insufficient permissions'})
                continue
            
            # Add to subscriptions
            if channel not in self.channel_subscriptions:
                self.channel_subscriptions[channel] = set()
            
            self.channel_subscriptions[channel].add(connection_id)
            connection.subscription_channels.add(channel)
            successful_subscriptions.append(channel)
        
        # Send subscription result
        await self._send_to_connection(
            connection_id,
            SecurityWebSocketMessage(
                event_type=WebSocketEventType.DASHBOARD_UPDATE,
                timestamp=datetime.now(),
                data={
                    'type': 'subscription_result',
                    'successful_subscriptions': successful_subscriptions,
                    'failed_subscriptions': failed_subscriptions,
                    'total_channels': len(connection.subscription_channels)
                }
            )
        )
    
    async def _unsubscribe_from_channel(self, connection_id: str, channel: str):
        """Unsubscribe connection from channel"""
        
        connection = self.connections.get(connection_id)
        if not connection:
            return
        
        if channel in self.channel_subscriptions:
            self.channel_subscriptions[channel].discard(connection_id)
            
            # Clean up empty channels
            if not self.channel_subscriptions[channel]:
                del self.channel_subscriptions[channel]
        
        connection.subscription_channels.discard(channel)
    
    def _get_default_channels(self, permissions: List[str]) -> List[str]:
        """Get default channels based on user permissions"""
        
        default_channels = []
        
        # Always include dashboard updates
        default_channels.append('dashboard_updates')
        
        # Add channels based on permissions
        for channel, required_perms in self.security_channels.items():
            if any(perm in permissions for perm in required_perms):
                default_channels.append(channel)
        
        return default_channels
    
    async def _is_authorized_for_message(
        self,
        connection: WebSocketConnection,
        message: SecurityWebSocketMessage
    ) -> bool:
        """Check if connection is authorized to receive message"""
        
        # Check permissions
        if message.requires_permissions:
            if not any(perm in connection.permissions for perm in message.requires_permissions):
                return False
        
        # Check clearance level
        classification_levels = {
            'public': 0,
            'internal': 1,
            'confidential': 2,
            'secret': 3,
            'top_secret': 4
        }
        
        required_clearance = classification_levels.get(message.security_classification, 0)
        if connection.clearance_level < required_clearance:
            return False
        
        return True
    
    async def _send_to_connection(self, connection_id: str, message: SecurityWebSocketMessage):
        """Send message to specific connection"""
        
        try:
            connection = self.connections.get(connection_id)
            if not connection or connection.websocket.client_state != WebSocketState.CONNECTED:
                return
            
            # Apply data redaction based on user context
            redacted_data = await self._apply_message_redaction(
                message.data,
                connection,
                message.redaction_level
            )
            
            # Prepare WebSocket message
            ws_message = {
                'event_type': message.event_type.value,
                'timestamp': message.timestamp.isoformat(),
                'data': redacted_data,
                'security_classification': message.security_classification,
                'metadata': message.metadata
            }
            
            await connection.websocket.send_text(json.dumps(ws_message))
            
        except WebSocketDisconnect:
            await self.disconnect_websocket(connection_id, "Client disconnected")
        except Exception as e:
            self.logger.error(f"Error sending message to {connection_id}: {e}")
    
    async def _apply_message_redaction(
        self,
        data: Dict[str, Any],
        connection: WebSocketConnection,
        redaction_level: str
    ) -> Dict[str, Any]:
        """Apply appropriate redaction to message data based on user context"""
        
        # For high clearance users, minimal redaction
        if connection.clearance_level >= 4:
            return data
        
        # For medium clearance users, standard redaction
        elif connection.clearance_level >= 2:
            return self._redact_sensitive_fields(data, level='standard')
        
        # For low clearance users, strict redaction
        else:
            return self._redact_sensitive_fields(data, level='strict')
    
    def _redact_sensitive_fields(self, data: Dict[str, Any], level: str = 'standard') -> Dict[str, Any]:
        """Redact sensitive fields from message data"""
        
        if not isinstance(data, dict):
            return data
        
        redacted_data = {}
        
        # Fields to redact based on level
        sensitive_fields = {
            'standard': ['ip_address', 'email', 'phone', 'user_agent'],
            'strict': ['ip_address', 'email', 'phone', 'user_agent', 'user_id', 'session_id', 'source']
        }
        
        fields_to_redact = sensitive_fields.get(level, [])
        
        for key, value in data.items():
            if key.lower() in fields_to_redact:
                if key.lower() == 'ip_address':
                    # Show only first octet
                    redacted_data[key] = self._redact_ip_address(str(value))
                elif key.lower() in ['email', 'user_id']:
                    redacted_data[key] = '[REDACTED]'
                else:
                    redacted_data[key] = '[REDACTED]'
            elif isinstance(value, dict):
                redacted_data[key] = self._redact_sensitive_fields(value, level)
            elif isinstance(value, list):
                redacted_data[key] = [
                    self._redact_sensitive_fields(item, level) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                redacted_data[key] = value
        
        return redacted_data
    
    def _redact_ip_address(self, ip: str) -> str:
        """Redact IP address showing only first octet"""
        parts = ip.split('.')
        return f"{parts[0]}.***.***.***" if len(parts) >= 4 else "[IP_REDACTED]"
    
    def _determine_redaction_level(self, security_classification: str) -> str:
        """Determine redaction level based on security classification"""
        
        classification_to_redaction = {
            'public': 'minimal',
            'internal': 'standard',
            'confidential': 'standard',
            'secret': 'strict',
            'top_secret': 'maximum'
        }
        
        return classification_to_redaction.get(security_classification, 'standard')
    
    async def _check_rate_limit(self, connection_id: str) -> bool:
        """Check if connection exceeds rate limits"""
        
        connection = self.connections.get(connection_id)
        if not connection:
            return False
        
        now = datetime.now()
        
        # Reset counter if minute has passed
        if now >= connection.rate_limit_reset:
            connection.rate_limit_count = 0
            connection.rate_limit_reset = now + timedelta(minutes=1)
        
        # Check limit
        if connection.rate_limit_count >= self.rate_limit_per_minute:
            return False
        
        connection.rate_limit_count += 1
        return True
    
    async def _send_error(self, connection_id: str, error_message: str):
        """Send error message to connection"""
        
        error_msg = SecurityWebSocketMessage(
            event_type=WebSocketEventType.DASHBOARD_UPDATE,
            timestamp=datetime.now(),
            data={
                'type': 'error',
                'error': error_message
            }
        )
        
        await self._send_to_connection(connection_id, error_msg)
    
    async def _send_dashboard_data(self, connection_id: str):
        """Send current dashboard data to connection"""
        
        try:
            # Get dashboard data from various sources
            security_summary = self.security_manager.get_security_dashboard()
            dlp_summary = self.dlp_manager.get_dlp_dashboard()
            compliance_summary = self.compliance_engine.get_compliance_dashboard()
            
            dashboard_data = {
                'type': 'dashboard_snapshot',
                'security_summary': security_summary,
                'dlp_summary': dlp_summary,
                'compliance_summary': compliance_summary,
                'timestamp': datetime.now().isoformat()
            }
            
            await self._send_to_connection(
                connection_id,
                SecurityWebSocketMessage(
                    event_type=WebSocketEventType.DASHBOARD_UPDATE,
                    timestamp=datetime.now(),
                    data=dashboard_data
                )
            )
            
        except Exception as e:
            self.logger.error(f"Error sending dashboard data: {e}")
            await self._send_error(connection_id, "Failed to retrieve dashboard data")
    
    async def _send_real_time_metrics(self, connection_id: str):
        """Send real-time metrics to connection"""
        
        try:
            metrics_data = {
                'type': 'real_time_metrics',
                'timestamp': datetime.now().isoformat(),
                'active_connections': len(self.connections),
                'active_threats': len(self.security_manager.security_events),
                'system_health': 'healthy',
                'processing_rate': 150.2,  # Events per second
                'memory_usage': 68.5,
                'cpu_usage': 45.2
            }
            
            await self._send_to_connection(
                connection_id,
                SecurityWebSocketMessage(
                    event_type=WebSocketEventType.REAL_TIME_METRICS,
                    timestamp=datetime.now(),
                    data=metrics_data
                )
            )
            
        except Exception as e:
            self.logger.error(f"Error sending real-time metrics: {e}")
    
    async def _handle_security_command(self, connection_id: str, command: Dict[str, Any]):
        """Handle security command from client"""
        
        connection = self.connections.get(connection_id)
        if not connection:
            return
        
        command_type = command.get('type')
        
        # Check if user has admin permissions for commands
        if 'perm_admin_system' not in connection.permissions:
            await self._send_error(connection_id, "Insufficient permissions for security commands")
            return
        
        if command_type == 'run_security_scan':
            # Trigger security scan
            result = await self.security_orchestrator.run_security_assessment()
            
            await self._send_to_connection(
                connection_id,
                SecurityWebSocketMessage(
                    event_type=WebSocketEventType.DASHBOARD_UPDATE,
                    timestamp=datetime.now(),
                    data={
                        'type': 'security_scan_result',
                        'result': result
                    }
                )
            )
        
        else:
            await self._send_error(connection_id, f"Unknown command type: {command_type}")
    
    def _get_client_ip(self, websocket: WebSocket) -> str:
        """Extract client IP from WebSocket"""
        return getattr(websocket.client, 'host', 'unknown')
    
    async def _log_websocket_event(self, connection_id: str, event_type: str, metadata: Dict[str, Any]):
        """Log WebSocket event for security monitoring"""
        
        try:
            self.security_manager.audit_logger.log_audit_event(
                user_id=metadata.get('user_id', 'unknown'),
                action='websocket_event',
                resource_type='websocket_connection',
                resource_id=connection_id,
                metadata={
                    'event_type': event_type,
                    'websocket_metadata': metadata
                }
            )
        except Exception as e:
            self.logger.error(f"Failed to log WebSocket event: {e}")
    
    # Background tasks
    
    async def _periodic_health_check(self):
        """Periodic health check for WebSocket connections"""
        
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                current_time = datetime.now()
                inactive_connections = []
                
                # Find inactive connections
                for connection_id, connection in self.connections.items():
                    if (current_time - connection.last_activity).total_seconds() > 300:  # 5 minutes
                        inactive_connections.append(connection_id)
                
                # Disconnect inactive connections
                for connection_id in inactive_connections:
                    await self.disconnect_websocket(connection_id, "Inactive connection cleanup")
                
                if inactive_connections:
                    self.logger.info(f"Cleaned up {len(inactive_connections)} inactive WebSocket connections")
                
            except Exception as e:
                self.logger.error(f"Health check error: {e}")
    
    async def _periodic_metrics_broadcast(self):
        """Periodic broadcast of real-time metrics"""
        
        while True:
            try:
                await asyncio.sleep(5)  # Broadcast every 5 seconds
                
                # Get connections subscribed to real-time metrics
                metrics_subscribers = self.channel_subscriptions.get('real_time_metrics', set())
                
                if metrics_subscribers:
                    metrics_data = {
                        'timestamp': datetime.now().isoformat(),
                        'active_connections': len(self.connections),
                        'active_threats': len(getattr(self.security_manager, 'security_events', [])),
                        'processing_rate': 150.0 + (hash(str(datetime.now())) % 50),  # Simulated
                        'memory_usage': 65.0 + (hash(str(datetime.now())) % 20),
                        'cpu_usage': 40.0 + (hash(str(datetime.now())) % 30)
                    }
                    
                    await self.broadcast_security_event(
                        event_type=WebSocketEventType.REAL_TIME_METRICS,
                        data=metrics_data,
                        channel='real_time_metrics',
                        security_classification='internal'
                    )
                
            except Exception as e:
                self.logger.error(f"Metrics broadcast error: {e}")


# Global WebSocket manager instance
_websocket_manager: Optional[SecurityWebSocketManager] = None


def get_websocket_manager() -> SecurityWebSocketManager:
    """Get global WebSocket manager instance"""
    global _websocket_manager
    if _websocket_manager is None:
        _websocket_manager = SecurityWebSocketManager()
    return _websocket_manager


# WebSocket endpoint handler
async def security_websocket_endpoint(websocket: WebSocket, token: str):
    """WebSocket endpoint for security dashboard"""
    
    manager = get_websocket_manager()
    connection_id = None
    
    try:
        # Establish connection
        connection_id = await manager.connect_websocket(websocket, token)
        
        if not connection_id:
            return  # Connection failed, already handled
        
        # Message handling loop
        while True:
            try:
                message = await websocket.receive_text()
                await manager.handle_websocket_message(connection_id, message)
                
            except WebSocketDisconnect:
                break
            except StarletteWebSocketDisconnect:
                break
            except Exception as e:
                logger.error(f"WebSocket message handling error: {e}")
                break
    
    except Exception as e:
        logger.error(f"WebSocket endpoint error: {e}")
    
    finally:
        if connection_id:
            await manager.disconnect_websocket(connection_id, "Connection closed")