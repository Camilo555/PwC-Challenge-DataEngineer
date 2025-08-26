"""
Secure WebSocket Implementation for Real-time Security Dashboard
Provides authenticated, encrypted, and monitored WebSocket connections for security events.
"""
import asyncio
import json
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

from fastapi import WebSocket, WebSocketDisconnect, HTTPException
from starlette.websockets import WebSocketState

from api.middleware.enterprise_security import get_websocket_security_middleware
from api.v1.services.enhanced_auth_service import get_auth_service
from core.logging import get_logger
from core.security.enterprise_security_orchestrator import get_security_orchestrator
from core.security.enhanced_access_control import get_access_control_manager
from core.security.security_dashboard import get_security_dashboard


logger = get_logger(__name__)


class SecureWebSocketConnection:
    """Secure WebSocket connection with authentication and monitoring"""
    
    def __init__(
        self,
        websocket: WebSocket,
        connection_id: str,
        user_id: str,
        permissions: List[str],
        session_id: str
    ):
        self.websocket = websocket
        self.connection_id = connection_id
        self.user_id = user_id
        self.permissions = permissions
        self.session_id = session_id
        self.connected_at = datetime.now()
        self.last_activity = datetime.now()
        self.message_count = 0
        self.subscriptions: Set[str] = set()
        self.rate_limit_window = []  # For rate limiting
        self.is_active = True
        
        self.logger = get_logger(f"{__name__}.{connection_id}")
    
    async def send_message(self, message_type: str, data: Any, metadata: Dict[str, Any] = None):
        """Send secure message to WebSocket client"""
        
        if not self.is_active or self.websocket.client_state == WebSocketState.DISCONNECTED:
            return False
        
        try:
            message = {
                'type': message_type,
                'data': data,
                'timestamp': datetime.now().isoformat(),
                'connection_id': self.connection_id,
                'message_id': str(uuid.uuid4()),
                'metadata': metadata or {}
            }
            
            await self.websocket.send_json(message)
            self.message_count += 1
            self.last_activity = datetime.now()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send message: {e}")
            self.is_active = False
            return False
    
    async def send_error(self, error_code: str, error_message: str, details: Dict[str, Any] = None):
        """Send error message to client"""
        
        await self.send_message(
            'error',
            {
                'error_code': error_code,
                'error_message': error_message,
                'details': details or {}
            }
        )
    
    def update_activity(self):
        """Update last activity timestamp"""
        self.last_activity = datetime.now()
    
    def add_subscription(self, topic: str) -> bool:
        """Add subscription to security event topic"""
        
        # Check permissions for topic
        if not self._check_topic_permission(topic):
            return False
        
        self.subscriptions.add(topic)
        return True
    
    def remove_subscription(self, topic: str):
        """Remove subscription from topic"""
        self.subscriptions.discard(topic)
    
    def _check_topic_permission(self, topic: str) -> bool:
        """Check if user has permission for specific topic"""
        
        topic_permissions = {
            'security_alerts': ['perm_admin_system', 'perm_audit_logs'],
            'compliance_violations': ['perm_compliance_report'],
            'dlp_incidents': ['perm_audit_logs'],
            'access_control_events': ['perm_admin_users'],
            'system_health': ['perm_admin_system'],
            'audit_logs': ['perm_audit_logs'],
            'threat_detection': ['perm_admin_system'],
            'dashboard_metrics': []  # Available to all authenticated users
        }
        
        required_perms = topic_permissions.get(topic, ['perm_admin_system'])
        
        # If no permissions required, allow all authenticated users
        if not required_perms:
            return True
        
        return any(perm in self.permissions for perm in required_perms)
    
    def is_rate_limited(self, max_messages: int = 10, window_seconds: int = 60) -> bool:
        """Check if connection is rate limited"""
        
        current_time = datetime.now()
        window_start = current_time - timedelta(seconds=window_seconds)
        
        # Clean old entries
        self.rate_limit_window = [
            timestamp for timestamp in self.rate_limit_window
            if timestamp > window_start
        ]
        
        # Check rate limit
        if len(self.rate_limit_window) >= max_messages:
            return True
        
        # Add current request
        self.rate_limit_window.append(current_time)
        return False
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get connection information for monitoring"""
        
        return {
            'connection_id': self.connection_id,
            'user_id': self.user_id,
            'session_id': self.session_id,
            'connected_at': self.connected_at.isoformat(),
            'last_activity': self.last_activity.isoformat(),
            'message_count': self.message_count,
            'subscriptions': list(self.subscriptions),
            'is_active': self.is_active,
            'connection_duration': (datetime.now() - self.connected_at).total_seconds()
        }


class SecurityDashboardWebSocketManager:
    """
    Manages secure WebSocket connections for the security dashboard
    Handles authentication, authorization, real-time updates, and monitoring
    """
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.connections: Dict[str, SecureWebSocketConnection] = {}
        self.topic_subscriptions: Dict[str, Set[str]] = {}  # topic -> connection_ids
        
        # Components
        self.websocket_security = get_websocket_security_middleware()
        self.auth_service = get_auth_service()
        self.security_orchestrator = get_security_orchestrator()
        self.access_manager = get_access_control_manager()
        self.security_dashboard = get_security_dashboard()
        
        # Configuration
        self.max_connections_per_user = 5
        self.heartbeat_interval = 30  # seconds
        self.cleanup_interval = 300   # seconds
        
        # Start background tasks
        self._start_background_tasks()
    
    def _start_background_tasks(self):
        """Start background maintenance tasks"""
        
        asyncio.create_task(self._heartbeat_task())
        asyncio.create_task(self._cleanup_task())
        asyncio.create_task(self._security_event_publisher())
    
    async def connect_client(
        self,
        websocket: WebSocket,
        token: Optional[str] = None
    ) -> SecureWebSocketConnection:
        """Establish secure WebSocket connection with authentication"""
        
        try:
            await websocket.accept()
            
            # Authenticate connection
            auth_result = await self.websocket_security.authenticate_websocket(websocket, token)
            
            if not auth_result['authenticated']:
                await websocket.send_json({
                    'type': 'auth_error',
                    'message': auth_result['reason'],
                    'timestamp': datetime.now().isoformat()
                })
                await websocket.close(code=1008, reason="Authentication failed")
                raise HTTPException(status_code=401, detail=auth_result['reason'])
            
            user_id = auth_result['user_id']
            connection_id = auth_result['connection_id']
            permissions = auth_result.get('permissions', [])
            
            # Check connection limits
            user_connections = [
                conn for conn in self.connections.values()
                if conn.user_id == user_id and conn.is_active
            ]
            
            if len(user_connections) >= self.max_connections_per_user:
                await websocket.send_json({
                    'type': 'connection_limit_exceeded',
                    'message': f'Maximum {self.max_connections_per_user} connections per user',
                    'timestamp': datetime.now().isoformat()
                })
                await websocket.close(code=1013, reason="Connection limit exceeded")
                raise HTTPException(status_code=429, detail="Too many connections")
            
            # Create secure connection
            connection = SecureWebSocketConnection(
                websocket=websocket,
                connection_id=connection_id,
                user_id=user_id,
                permissions=permissions,
                session_id=str(uuid.uuid4())
            )
            
            # Store connection
            self.connections[connection_id] = connection
            
            # Send welcome message with initial data
            await self._send_welcome_message(connection)
            
            # Log connection
            self.logger.info(
                f"WebSocket connected: {connection_id} (user: {user_id})",
                extra={
                    'connection_id': connection_id,
                    'user_id': user_id,
                    'permissions': len(permissions)
                }
            )
            
            return connection
            
        except Exception as e:
            self.logger.error(f"WebSocket connection failed: {e}")
            try:
                if websocket.client_state != WebSocketState.DISCONNECTED:
                    await websocket.close(code=1011, reason="Connection error")
            except:
                pass
            raise
    
    async def disconnect_client(self, connection_id: str, reason: str = "Client disconnect"):
        """Disconnect WebSocket client"""
        
        if connection_id in self.connections:
            connection = self.connections[connection_id]
            
            try:
                # Remove from all subscriptions
                for topic in list(connection.subscriptions):
                    self._remove_from_topic(connection_id, topic)
                
                # Mark as inactive
                connection.is_active = False
                
                # Close websocket if still open
                if connection.websocket.client_state != WebSocketState.DISCONNECTED:
                    await connection.websocket.close(code=1000, reason=reason)
                
                # Log disconnection
                connection_info = connection.get_connection_info()
                self.logger.info(
                    f"WebSocket disconnected: {connection_id} (reason: {reason})",
                    extra={
                        'connection_id': connection_id,
                        'user_id': connection.user_id,
                        'duration': connection_info['connection_duration']
                    }
                )
                
                # Remove from active connections
                del self.connections[connection_id]
                
            except Exception as e:
                self.logger.error(f"Error disconnecting client {connection_id}: {e}")
    
    async def handle_client_message(
        self,
        connection: SecureWebSocketConnection,
        message: Dict[str, Any]
    ):
        """Handle incoming message from WebSocket client"""
        
        try:
            # Rate limiting check
            if connection.is_rate_limited():
                await connection.send_error(
                    'rate_limit_exceeded',
                    'Too many messages sent'
                )
                return
            
            connection.update_activity()
            
            message_type = message.get('type')
            data = message.get('data', {})
            
            if message_type == 'subscribe':
                await self._handle_subscribe(connection, data)
            elif message_type == 'unsubscribe':
                await self._handle_unsubscribe(connection, data)
            elif message_type == 'get_dashboard_data':
                await self._handle_get_dashboard_data(connection, data)
            elif message_type == 'run_security_scan':
                await self._handle_security_scan(connection, data)
            elif message_type == 'ping':
                await connection.send_message('pong', {'timestamp': datetime.now().isoformat()})
            else:
                await connection.send_error(
                    'unknown_message_type',
                    f'Unknown message type: {message_type}'
                )
        
        except Exception as e:
            self.logger.error(f"Error handling client message: {e}")
            await connection.send_error(
                'message_processing_error',
                'Failed to process message'
            )
    
    async def _handle_subscribe(self, connection: SecureWebSocketConnection, data: Dict[str, Any]):
        """Handle subscription request"""
        
        topic = data.get('topic')
        if not topic:
            await connection.send_error('invalid_request', 'Topic is required')
            return
        
        if connection.add_subscription(topic):
            self._add_to_topic(connection.connection_id, topic)
            
            await connection.send_message(
                'subscription_confirmed',
                {'topic': topic},
                {'subscription_count': len(connection.subscriptions)}
            )
            
            # Send initial data for the topic
            await self._send_topic_initial_data(connection, topic)
            
        else:
            await connection.send_error(
                'subscription_denied',
                f'Permission denied for topic: {topic}'
            )
    
    async def _handle_unsubscribe(self, connection: SecureWebSocketConnection, data: Dict[str, Any]):
        """Handle unsubscription request"""
        
        topic = data.get('topic')
        if not topic:
            await connection.send_error('invalid_request', 'Topic is required')
            return
        
        connection.remove_subscription(topic)
        self._remove_from_topic(connection.connection_id, topic)
        
        await connection.send_message(
            'unsubscription_confirmed',
            {'topic': topic},
            {'subscription_count': len(connection.subscriptions)}
        )
    
    async def _handle_get_dashboard_data(self, connection: SecureWebSocketConnection, data: Dict[str, Any]):
        """Handle dashboard data request"""
        
        try:
            dashboard_data = await self.security_orchestrator.get_unified_dashboard(connection.user_id)
            
            await connection.send_message(
                'dashboard_data',
                dashboard_data,
                {'data_timestamp': datetime.now().isoformat()}
            )
            
        except Exception as e:
            await connection.send_error(
                'dashboard_data_error',
                f'Failed to retrieve dashboard data: {str(e)}'
            )
    
    async def _handle_security_scan(self, connection: SecureWebSocketConnection, data: Dict[str, Any]):
        """Handle security scan request"""
        
        # Check permissions
        if 'perm_admin_system' not in connection.permissions:
            await connection.send_error(
                'permission_denied',
                'Administrative privileges required'
            )
            return
        
        try:
            # Run security assessment
            assessment = await self.security_orchestrator.run_security_assessment()
            
            await connection.send_message(
                'security_scan_result',
                assessment,
                {
                    'scan_requested_by': connection.user_id,
                    'scan_timestamp': datetime.now().isoformat()
                }
            )
            
        except Exception as e:
            await connection.send_error(
                'security_scan_error',
                f'Security scan failed: {str(e)}'
            )
    
    async def broadcast_to_topic(
        self,
        topic: str,
        message_type: str,
        data: Any,
        metadata: Dict[str, Any] = None
    ):
        """Broadcast message to all subscribers of a topic"""
        
        if topic not in self.topic_subscriptions:
            return
        
        subscriber_ids = self.topic_subscriptions[topic].copy()
        
        for connection_id in subscriber_ids:
            if connection_id in self.connections:
                connection = self.connections[connection_id]
                
                if connection.is_active:
                    success = await connection.send_message(message_type, data, metadata)
                    
                    if not success:
                        # Remove inactive connection
                        await self.disconnect_client(connection_id, "Send failed")
                else:
                    # Clean up inactive connection
                    await self.disconnect_client(connection_id, "Inactive connection")
    
    async def _send_welcome_message(self, connection: SecureWebSocketConnection):
        """Send welcome message with connection info"""
        
        await connection.send_message(
            'welcome',
            {
                'connection_id': connection.connection_id,
                'user_id': connection.user_id,
                'permissions': connection.permissions,
                'server_time': datetime.now().isoformat(),
                'available_topics': [
                    'security_alerts',
                    'compliance_violations',
                    'dlp_incidents',
                    'access_control_events',
                    'system_health',
                    'audit_logs',
                    'threat_detection',
                    'dashboard_metrics'
                ]
            },
            {
                'connection_established': True,
                'heartbeat_interval': self.heartbeat_interval
            }
        )
    
    async def _send_topic_initial_data(self, connection: SecureWebSocketConnection, topic: str):
        """Send initial data for newly subscribed topic"""
        
        try:
            initial_data = {}
            
            if topic == 'dashboard_metrics':
                initial_data = await self.security_orchestrator.get_unified_dashboard(connection.user_id)
            elif topic == 'security_alerts':
                summary = self.security_orchestrator.get_security_summary()
                initial_data = {
                    'active_alerts': summary.get('key_metrics', {}).get('active_alerts', 0),
                    'critical_alerts': summary.get('key_metrics', {}).get('critical_alerts', 0)
                }
            elif topic == 'system_health':
                initial_data = {
                    'platform_status': 'operational',
                    'components_online': True,
                    'last_updated': datetime.now().isoformat()
                }
            
            if initial_data:
                await connection.send_message(
                    f'{topic}_initial_data',
                    initial_data,
                    {'topic': topic, 'initial_data': True}
                )
                
        except Exception as e:
            self.logger.error(f"Failed to send initial data for topic {topic}: {e}")
    
    def _add_to_topic(self, connection_id: str, topic: str):
        """Add connection to topic subscription"""
        
        if topic not in self.topic_subscriptions:
            self.topic_subscriptions[topic] = set()
        
        self.topic_subscriptions[topic].add(connection_id)
    
    def _remove_from_topic(self, connection_id: str, topic: str):
        """Remove connection from topic subscription"""
        
        if topic in self.topic_subscriptions:
            self.topic_subscriptions[topic].discard(connection_id)
            
            # Clean up empty topics
            if not self.topic_subscriptions[topic]:
                del self.topic_subscriptions[topic]
    
    async def _heartbeat_task(self):
        """Send heartbeat messages to maintain connections"""
        
        while True:
            try:
                await asyncio.sleep(self.heartbeat_interval)
                
                current_time = datetime.now()
                
                for connection_id, connection in list(self.connections.items()):
                    if connection.is_active:
                        # Check if connection is still responsive
                        time_since_activity = (current_time - connection.last_activity).total_seconds()
                        
                        if time_since_activity > (self.heartbeat_interval * 3):  # 3x heartbeat interval
                            # Connection seems unresponsive
                            await self.disconnect_client(connection_id, "Heartbeat timeout")
                        else:
                            # Send heartbeat
                            await connection.send_message(
                                'heartbeat',
                                {
                                    'server_time': current_time.isoformat(),
                                    'connection_duration': time_since_activity
                                }
                            )
            
            except Exception as e:
                self.logger.error(f"Heartbeat task error: {e}")
    
    async def _cleanup_task(self):
        """Clean up inactive connections and expired data"""
        
        while True:
            try:
                await asyncio.sleep(self.cleanup_interval)
                
                current_time = datetime.now()
                inactive_connections = []
                
                for connection_id, connection in self.connections.items():
                    # Check for inactive connections
                    if not connection.is_active:
                        inactive_connections.append(connection_id)
                    else:
                        # Check for very old connections (24 hours)
                        connection_age = (current_time - connection.connected_at).total_seconds()
                        if connection_age > 86400:  # 24 hours
                            inactive_connections.append(connection_id)
                
                # Clean up inactive connections
                for connection_id in inactive_connections:
                    await self.disconnect_client(connection_id, "Cleanup - inactive")
                
                if inactive_connections:
                    self.logger.info(f"Cleaned up {len(inactive_connections)} inactive connections")
                
                # Clean up WebSocket security middleware inactive connections
                await self.websocket_security.cleanup_inactive_connections()
                
            except Exception as e:
                self.logger.error(f"Cleanup task error: {e}")
    
    async def _security_event_publisher(self):
        """Publish real-time security events to subscribers"""
        
        while True:
            try:
                await asyncio.sleep(10)  # Check every 10 seconds
                
                # Get recent security events (this would integrate with actual security event sources)
                current_time = datetime.now()
                
                # Mock security events - in production, this would come from actual security systems
                mock_events = [
                    {
                        'type': 'dlp_detection',
                        'severity': 'medium',
                        'message': 'PII detected in API response',
                        'timestamp': current_time.isoformat(),
                        'details': {'endpoint': '/api/v1/sales/customer-data', 'data_type': 'email'}
                    },
                    {
                        'type': 'access_denied',
                        'severity': 'low',
                        'message': 'Access denied to restricted resource',
                        'timestamp': current_time.isoformat(),
                        'details': {'user_id': 'user123', 'resource': '/admin/settings'}
                    }
                ]
                
                # Broadcast events to appropriate topics
                for event in mock_events:
                    if event['type'] == 'dlp_detection':
                        await self.broadcast_to_topic(
                            'dlp_incidents',
                            'dlp_incident',
                            event,
                            {'real_time': True}
                        )
                    elif event['type'] == 'access_denied':
                        await self.broadcast_to_topic(
                            'access_control_events',
                            'access_event',
                            event,
                            {'real_time': True}
                        )
                
                # Update dashboard metrics
                if self.topic_subscriptions.get('dashboard_metrics'):
                    dashboard_summary = self.security_orchestrator.get_security_summary()
                    await self.broadcast_to_topic(
                        'dashboard_metrics',
                        'metrics_update',
                        dashboard_summary,
                        {
                            'update_type': 'periodic',
                            'timestamp': current_time.isoformat()
                        }
                    )
                
            except Exception as e:
                self.logger.error(f"Security event publisher error: {e}")
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """Get WebSocket connection statistics"""
        
        active_connections = [conn for conn in self.connections.values() if conn.is_active]
        
        user_counts = {}
        for conn in active_connections:
            user_counts[conn.user_id] = user_counts.get(conn.user_id, 0) + 1
        
        topic_stats = {}
        for topic, subscribers in self.topic_subscriptions.items():
            topic_stats[topic] = len(subscribers)
        
        return {
            'total_connections': len(self.connections),
            'active_connections': len(active_connections),
            'unique_users': len(user_counts),
            'connections_per_user': user_counts,
            'topic_subscriptions': topic_stats,
            'average_session_duration': sum(
                (datetime.now() - conn.connected_at).total_seconds()
                for conn in active_connections
            ) / len(active_connections) if active_connections else 0
        }


# Global WebSocket manager
_websocket_manager: Optional[SecurityDashboardWebSocketManager] = None

def get_websocket_manager() -> SecurityDashboardWebSocketManager:
    """Get global WebSocket manager instance"""
    global _websocket_manager
    if _websocket_manager is None:
        _websocket_manager = SecurityDashboardWebSocketManager()
    return _websocket_manager


async def handle_websocket_connection(websocket: WebSocket, token: Optional[str] = None):
    """Main WebSocket connection handler"""
    
    websocket_manager = get_websocket_manager()
    connection = None
    
    try:
        # Establish secure connection
        connection = await websocket_manager.connect_client(websocket, token)
        
        # Handle messages
        while connection.is_active and websocket.client_state == WebSocketState.CONNECTED:
            try:
                # Receive message with timeout
                message = await asyncio.wait_for(
                    websocket.receive_json(),
                    timeout=300.0  # 5 minute timeout
                )
                
                await websocket_manager.handle_client_message(connection, message)
                
            except asyncio.TimeoutError:
                # Send ping to check if connection is alive
                await connection.send_message('ping', {'timestamp': datetime.now().isoformat()})
                
            except WebSocketDisconnect:
                break
                
            except Exception as e:
                logger.error(f"Error handling WebSocket message: {e}")
                await connection.send_error('message_error', str(e))
    
    except WebSocketDisconnect:
        pass  # Normal disconnection
    
    except Exception as e:
        logger.error(f"WebSocket connection error: {e}")
        
    finally:
        # Clean up connection
        if connection:
            await websocket_manager.disconnect_client(
                connection.connection_id,
                "Connection closed"
            )