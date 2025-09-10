"""
Real-Time Dashboard WebSocket API
Provides WebSocket endpoints for real-time business intelligence dashboard updates
"""
import asyncio
import json
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass, asdict
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import redis.asyncio as aioredis

from src.api.auth.jwt_handler import verify_jwt_token, get_current_user
from src.api.dashboard.dashboard_cache_manager import create_dashboard_cache_manager
from src.streaming.real_time_dashboard_processor import create_real_time_dashboard_processor
from src.core.database.async_db_manager import get_async_db_session
from core.config.unified_config import get_unified_config
from core.logging import get_logger


@dataclass
class WebSocketMessage:
    """WebSocket message structure"""
    type: str
    data: Dict[str, Any]
    timestamp: str
    user_id: str
    session_id: str
    message_id: str = None
    
    def __post_init__(self):
        if self.message_id is None:
            self.message_id = str(uuid.uuid4())
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class DashboardWebSocketManager:
    """
    WebSocket connection manager for real-time dashboard updates
    Handles multiple user connections with intelligent message routing
    """
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.config = get_unified_config()
        
        # Active connections by user_id
        self.connections: Dict[str, Set[WebSocket]] = {}
        
        # Session tracking
        self.sessions: Dict[str, Dict[str, Any]] = {}
        
        # Message history for replay
        self.message_history: Dict[str, List[WebSocketMessage]] = {}
        
        # Dashboard subscriptions (user -> dashboard types)
        self.subscriptions: Dict[str, Set[str]] = {}
        
        # Performance metrics
        self.metrics = {
            "active_connections": 0,
            "messages_sent": 0,
            "messages_failed": 0,
            "average_latency_ms": 0.0,
            "peak_connections": 0
        }
        
        # Initialize components
        self.dashboard_cache = create_dashboard_cache_manager()
        self.dashboard_processor = create_real_time_dashboard_processor()
        
        # Register with processor for real-time updates
        self._register_processor_callbacks()
    
    def _register_processor_callbacks(self):
        """Register callbacks with the dashboard processor"""
        # This would be implemented to receive real-time updates
        pass
    
    async def connect_user(self, websocket: WebSocket, user_id: str, dashboard_types: List[str]) -> str:
        """Connect a user's WebSocket with dashboard subscriptions"""
        try:
            await websocket.accept()
            
            # Generate session ID
            session_id = str(uuid.uuid4())
            
            # Add to connections
            if user_id not in self.connections:
                self.connections[user_id] = set()
            self.connections[user_id].add(websocket)
            
            # Track session
            self.sessions[session_id] = {
                "user_id": user_id,
                "websocket": websocket,
                "connected_at": datetime.now(),
                "dashboard_types": dashboard_types,
                "last_activity": datetime.now()
            }
            
            # Set up subscriptions
            if user_id not in self.subscriptions:
                self.subscriptions[user_id] = set()
            self.subscriptions[user_id].update(dashboard_types)
            
            # Update metrics
            self.metrics["active_connections"] = len(self.sessions)
            self.metrics["peak_connections"] = max(
                self.metrics["peak_connections"], 
                self.metrics["active_connections"]
            )
            
            # Send welcome message with current KPIs
            await self._send_welcome_message(websocket, user_id, session_id)
            
            # Register with dashboard processor for real-time updates
            self.dashboard_processor.register_websocket_connection(user_id, websocket)
            
            self.logger.info(f"User {user_id} connected with session {session_id}, subscriptions: {dashboard_types}")
            return session_id
            
        except Exception as e:
            self.logger.error(f"Error connecting user {user_id}: {e}")
            raise HTTPException(status_code=500, detail="Connection failed")
    
    async def disconnect_user(self, user_id: str, websocket: WebSocket, session_id: str):
        """Disconnect a user's WebSocket"""
        try:
            # Remove from connections
            if user_id in self.connections:
                self.connections[user_id].discard(websocket)
                if not self.connections[user_id]:
                    del self.connections[user_id]
                    # Clear subscriptions if no connections left
                    self.subscriptions.pop(user_id, None)
            
            # Remove session
            self.sessions.pop(session_id, None)
            
            # Unregister from processor
            self.dashboard_processor.unregister_websocket_connection(user_id, websocket)
            
            # Update metrics
            self.metrics["active_connections"] = len(self.sessions)
            
            self.logger.info(f"User {user_id} disconnected, session {session_id} ended")
            
        except Exception as e:
            self.logger.error(f"Error disconnecting user {user_id}: {e}")
    
    async def _send_welcome_message(self, websocket: WebSocket, user_id: str, session_id: str):
        """Send welcome message with initial dashboard data"""
        try:
            # Get current KPIs based on user subscriptions
            dashboard_types = self.subscriptions.get(user_id, set())
            initial_data = {}
            
            for dashboard_type in dashboard_types:
                data = await self._get_dashboard_data(dashboard_type, user_id)
                if data:
                    initial_data[dashboard_type] = data
            
            welcome_message = WebSocketMessage(
                type="welcome",
                data={
                    "session_id": session_id,
                    "user_id": user_id,
                    "subscriptions": list(dashboard_types),
                    "initial_data": initial_data,
                    "server_time": datetime.now().isoformat(),
                    "capabilities": [
                        "real_time_kpis",
                        "anomaly_detection", 
                        "interactive_filters",
                        "alert_notifications"
                    ]
                },
                timestamp=datetime.now().isoformat(),
                user_id=user_id,
                session_id=session_id
            )
            
            await websocket.send_text(json.dumps(welcome_message.to_dict()))
            self.logger.debug(f"Sent welcome message to user {user_id}")
            
        except Exception as e:
            self.logger.error(f"Error sending welcome message to {user_id}: {e}")
    
    async def _get_dashboard_data(self, dashboard_type: str, user_id: str) -> Optional[Dict[str, Any]]:
        """Get dashboard data from cache"""
        try:
            # Map dashboard types to cache names
            cache_mapping = {
                "executive": "executive_kpis",
                "revenue": "revenue_analytics", 
                "operations": "operational_metrics",
                "customers": "customer_behavior",
                "quality": "data_quality",
                "performance": "api_performance",
                "financial": "financial_kpis"
            }
            
            cache_name = cache_mapping.get(dashboard_type)
            if not cache_name:
                return None
            
            # Get data from dashboard cache
            data = await self.dashboard_cache.get_dashboard_data(
                cache_name=cache_name,
                key="current",
                user_id=user_id
            )
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error getting dashboard data for {dashboard_type}: {e}")
            return None
    
    async def broadcast_update(self, update_type: str, data: Dict[str, Any], target_dashboards: List[str] = None):
        """Broadcast update to all connected clients or specific dashboard types"""
        try:
            message = WebSocketMessage(
                type=update_type,
                data=data,
                timestamp=datetime.now().isoformat(),
                user_id="system",
                session_id="broadcast"
            )
            
            sent_count = 0
            failed_count = 0
            
            # Send to all relevant users
            for user_id, user_subscriptions in self.subscriptions.items():
                # Check if user is subscribed to relevant dashboard types
                if target_dashboards:
                    if not any(dashboard in user_subscriptions for dashboard in target_dashboards):
                        continue
                
                # Send to all user connections
                if user_id in self.connections:
                    for websocket in self.connections[user_id].copy():
                        try:
                            await websocket.send_text(json.dumps(message.to_dict()))
                            sent_count += 1
                        except Exception as e:
                            self.logger.error(f"Failed to send to user {user_id}: {e}")
                            failed_count += 1
                            # Remove failed connection
                            self.connections[user_id].discard(websocket)
            
            # Update metrics
            self.metrics["messages_sent"] += sent_count
            self.metrics["messages_failed"] += failed_count
            
            self.logger.debug(f"Broadcasted {update_type} to {sent_count} connections, {failed_count} failed")
            
        except Exception as e:
            self.logger.error(f"Error broadcasting update: {e}")
    
    async def send_user_message(self, user_id: str, message_type: str, data: Dict[str, Any]) -> bool:
        """Send message to specific user"""
        try:
            if user_id not in self.connections:
                return False
            
            message = WebSocketMessage(
                type=message_type,
                data=data,
                timestamp=datetime.now().isoformat(),
                user_id=user_id,
                session_id="direct"
            )
            
            sent_count = 0
            for websocket in self.connections[user_id].copy():
                try:
                    await websocket.send_text(json.dumps(message.to_dict()))
                    sent_count += 1
                except Exception as e:
                    self.logger.error(f"Failed to send to user {user_id}: {e}")
                    self.connections[user_id].discard(websocket)
            
            return sent_count > 0
            
        except Exception as e:
            self.logger.error(f"Error sending message to user {user_id}: {e}")
            return False
    
    async def handle_client_message(self, websocket: WebSocket, user_id: str, session_id: str, message: Dict[str, Any]):
        """Handle incoming message from client"""
        try:
            message_type = message.get("type")
            data = message.get("data", {})
            
            # Update last activity
            if session_id in self.sessions:
                self.sessions[session_id]["last_activity"] = datetime.now()
            
            if message_type == "ping":
                await self._handle_ping(websocket, user_id)
            elif message_type == "subscribe":
                await self._handle_subscribe(user_id, data)
            elif message_type == "unsubscribe":
                await self._handle_unsubscribe(user_id, data)
            elif message_type == "get_kpi":
                await self._handle_get_kpi(websocket, user_id, data)
            elif message_type == "filter_data":
                await self._handle_filter_data(websocket, user_id, data)
            elif message_type == "set_alert":
                await self._handle_set_alert(user_id, data)
            else:
                self.logger.warning(f"Unknown message type: {message_type} from user {user_id}")
                
        except Exception as e:
            self.logger.error(f"Error handling client message from {user_id}: {e}")
    
    async def _handle_ping(self, websocket: WebSocket, user_id: str):
        """Handle ping message"""
        pong_message = {
            "type": "pong",
            "timestamp": datetime.now().isoformat(),
            "server_time": datetime.now().isoformat()
        }
        await websocket.send_text(json.dumps(pong_message))
    
    async def _handle_subscribe(self, user_id: str, data: Dict[str, Any]):
        """Handle subscription request"""
        dashboard_types = data.get("dashboard_types", [])
        if user_id not in self.subscriptions:
            self.subscriptions[user_id] = set()
        self.subscriptions[user_id].update(dashboard_types)
        
        # Send current data for new subscriptions
        for dashboard_type in dashboard_types:
            dashboard_data = await self._get_dashboard_data(dashboard_type, user_id)
            if dashboard_data:
                await self.send_user_message(user_id, "dashboard_data", {
                    "dashboard_type": dashboard_type,
                    "data": dashboard_data
                })
    
    async def _handle_unsubscribe(self, user_id: str, data: Dict[str, Any]):
        """Handle unsubscribe request"""
        dashboard_types = data.get("dashboard_types", [])
        if user_id in self.subscriptions:
            for dashboard_type in dashboard_types:
                self.subscriptions[user_id].discard(dashboard_type)
    
    async def _handle_get_kpi(self, websocket: WebSocket, user_id: str, data: Dict[str, Any]):
        """Handle KPI data request"""
        kpi_name = data.get("kpi_name")
        timeframe = data.get("timeframe", "1h")
        
        if kpi_name:
            # Get KPI data from cache
            kpi_data = await self.dashboard_cache.get_dashboard_data(
                cache_name="executive_kpis",
                key=f"{kpi_name}_{timeframe}",
                user_id=user_id
            )
            
            response = {
                "type": "kpi_response",
                "data": {
                    "kpi_name": kpi_name,
                    "timeframe": timeframe,
                    "data": kpi_data,
                    "timestamp": datetime.now().isoformat()
                },
                "timestamp": datetime.now().isoformat()
            }
            
            await websocket.send_text(json.dumps(response))
    
    async def _handle_filter_data(self, websocket: WebSocket, user_id: str, data: Dict[str, Any]):
        """Handle data filtering request"""
        dashboard_type = data.get("dashboard_type")
        filters = data.get("filters", {})
        
        if dashboard_type:
            filtered_data = await self._get_dashboard_data(dashboard_type, user_id)
            # Apply filters (simplified implementation)
            
            response = {
                "type": "filtered_data",
                "data": {
                    "dashboard_type": dashboard_type,
                    "filters": filters,
                    "data": filtered_data
                },
                "timestamp": datetime.now().isoformat()
            }
            
            await websocket.send_text(json.dumps(response))
    
    async def _handle_set_alert(self, user_id: str, data: Dict[str, Any]):
        """Handle alert configuration"""
        kpi_name = data.get("kpi_name")
        threshold = data.get("threshold")
        condition = data.get("condition")  # "above", "below", "change"
        
        # Store alert configuration (simplified)
        alert_config = {
            "user_id": user_id,
            "kpi_name": kpi_name,
            "threshold": threshold,
            "condition": condition,
            "created_at": datetime.now().isoformat()
        }
        
        # In a real implementation, this would be stored in the database
        self.logger.info(f"Alert configured for user {user_id}: {alert_config}")
    
    async def cleanup_inactive_sessions(self):
        """Clean up inactive sessions"""
        try:
            cutoff_time = datetime.now() - timedelta(minutes=30)
            inactive_sessions = []
            
            for session_id, session_info in self.sessions.items():
                if session_info["last_activity"] < cutoff_time:
                    inactive_sessions.append(session_id)
            
            for session_id in inactive_sessions:
                session_info = self.sessions.pop(session_id, {})
                user_id = session_info.get("user_id")
                websocket = session_info.get("websocket")
                
                if user_id and websocket:
                    await self.disconnect_user(user_id, websocket, session_id)
            
            if inactive_sessions:
                self.logger.info(f"Cleaned up {len(inactive_sessions)} inactive sessions")
                
        except Exception as e:
            self.logger.error(f"Error cleaning up inactive sessions: {e}")
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """Get WebSocket connection statistics"""
        return {
            "active_connections": self.metrics["active_connections"],
            "peak_connections": self.metrics["peak_connections"],
            "messages_sent": self.metrics["messages_sent"],
            "messages_failed": self.metrics["messages_failed"],
            "success_rate": (
                self.metrics["messages_sent"] / 
                (self.metrics["messages_sent"] + self.metrics["messages_failed"])
            ) * 100 if (self.metrics["messages_sent"] + self.metrics["messages_failed"]) > 0 else 100,
            "connected_users": len(self.connections),
            "total_subscriptions": sum(len(subs) for subs in self.subscriptions.values()),
            "active_sessions": len(self.sessions)
        }


# Global WebSocket manager instance
ws_manager = DashboardWebSocketManager()

# WebSocket router
router = APIRouter(prefix="/ws/dashboard", tags=["WebSocket Dashboard"])
security = HTTPBearer()


async def verify_websocket_token(websocket: WebSocket, token: str) -> Optional[str]:
    """Verify WebSocket connection token"""
    try:
        # Extract token from query parameter or header
        if not token:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return None
            
        # Verify JWT token
        payload = verify_jwt_token(token)
        if not payload:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return None
            
        return payload.get("user_id")
        
    except Exception as e:
        logging.error(f"WebSocket token verification failed: {e}")
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return None


@router.websocket("/realtime/{user_id}")
async def dashboard_websocket_endpoint(
    websocket: WebSocket,
    user_id: str,
    token: str = None,
    dashboards: str = "executive,revenue"  # Comma-separated dashboard types
):
    """
    Real-time dashboard WebSocket endpoint
    
    Provides real-time updates for business intelligence dashboards including:
    - Executive KPIs
    - Revenue analytics
    - Operational metrics
    - Customer behavior
    - Data quality metrics
    - Performance monitoring
    """
    session_id = None
    
    try:
        # Verify authentication
        authenticated_user_id = await verify_websocket_token(websocket, token)
        if not authenticated_user_id or authenticated_user_id != user_id:
            return
        
        # Parse dashboard subscriptions
        dashboard_types = [d.strip() for d in dashboards.split(",") if d.strip()]
        
        # Connect user
        session_id = await ws_manager.connect_user(websocket, user_id, dashboard_types)
        
        # Message handling loop
        while True:
            try:
                # Receive message from client
                data = await websocket.receive_text()
                message = json.loads(data)
                
                # Handle the message
                await ws_manager.handle_client_message(websocket, user_id, session_id, message)
                
            except WebSocketDisconnect:
                break
            except json.JSONDecodeError:
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "message": "Invalid JSON format",
                    "timestamp": datetime.now().isoformat()
                }))
            except Exception as e:
                logging.error(f"Error in WebSocket message loop: {e}")
                await websocket.send_text(json.dumps({
                    "type": "error", 
                    "message": "Internal server error",
                    "timestamp": datetime.now().isoformat()
                }))
                
    except Exception as e:
        logging.error(f"WebSocket connection error for user {user_id}: {e}")
    finally:
        # Clean up connection
        if session_id:
            await ws_manager.disconnect_user(user_id, websocket, session_id)


@router.get("/stats")
async def get_websocket_stats():
    """Get WebSocket connection statistics"""
    return ws_manager.get_connection_stats()


@router.post("/broadcast")
async def broadcast_message(
    message_type: str,
    data: Dict[str, Any],
    target_dashboards: Optional[List[str]] = None,
    current_user: Dict = Depends(get_current_user)
):
    """
    Broadcast message to dashboard WebSocket connections
    Requires admin role
    """
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    await ws_manager.broadcast_update(message_type, data, target_dashboards)
    return {"message": "Broadcast sent successfully"}


# Background tasks for WebSocket management
async def websocket_maintenance():
    """Background task for WebSocket maintenance"""
    while True:
        try:
            await ws_manager.cleanup_inactive_sessions()
            await asyncio.sleep(300)  # Run every 5 minutes
        except Exception as e:
            logging.error(f"Error in WebSocket maintenance: {e}")
            await asyncio.sleep(60)


# Start background tasks
asyncio.create_task(websocket_maintenance())


# Export the WebSocket manager for use in other modules
__all__ = ["router", "ws_manager", "DashboardWebSocketManager"]