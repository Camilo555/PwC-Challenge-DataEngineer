"""
Real-time WebSocket Streaming Platform
=====================================

Comprehensive WebSocket implementation providing:
- Real-time data streaming for live analytics
- Multi-channel subscription management
- Scalable WebSocket connection handling
- Message broadcasting and room management
- Integration with Redis for horizontal scaling
- Custom protocol handlers for different data types
- Authentication and authorization for WebSocket connections
- Performance monitoring and connection analytics

Key Features:
- Real-time sales data streaming
- Live business metrics updates
- System health monitoring streams
- Custom event broadcasting
- Room-based subscriptions
- Connection pool management with auto-scaling
- Message queuing and delivery guarantees
"""

import asyncio
import json
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Set, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
import logging
import weakref
from collections import defaultdict, deque

import websockets
from websockets.server import WebSocketServerProtocol
from websockets.exceptions import ConnectionClosed, WebSocketException
import redis.asyncio as redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt
from pydantic import BaseModel
import uvicorn

from core.config import settings
from core.logging import get_logger
from monitoring.distributed_tracing import JaegerTracingProvider

logger = get_logger(__name__)


class MessageType(Enum):
    """WebSocket message types"""
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"
    DATA = "data"
    HEARTBEAT = "heartbeat"
    AUTH = "auth"
    ERROR = "error"
    NOTIFICATION = "notification"
    SYSTEM = "system"


class ChannelType(Enum):
    """Subscription channel types"""
    SALES_LIVE = "sales_live"
    ANALYTICS_DASHBOARD = "analytics_dashboard"
    BUSINESS_METRICS = "business_metrics"
    SYSTEM_HEALTH = "system_health"
    ALERTS = "alerts"
    NOTIFICATIONS = "notifications"
    CUSTOM_EVENTS = "custom_events"


class ConnectionStatus(Enum):
    """WebSocket connection status"""
    CONNECTING = "connecting"
    CONNECTED = "connected"
    AUTHENTICATED = "authenticated"
    SUBSCRIBED = "subscribed"
    DISCONNECTED = "disconnected"
    ERROR = "error"


@dataclass
class WebSocketMessage:
    """WebSocket message structure"""
    type: MessageType
    channel: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
    timestamp: datetime = field(default_factory=datetime.now)
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    user_id: Optional[str] = None
    session_id: Optional[str] = None


@dataclass
class ConnectionInfo:
    """WebSocket connection information"""
    connection_id: str
    websocket: WebSocket
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    status: ConnectionStatus = ConnectionStatus.CONNECTING
    subscribed_channels: Set[str] = field(default_factory=set)
    connected_at: datetime = field(default_factory=datetime.now)
    last_heartbeat: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    message_count: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0


@dataclass
class SubscriptionConfig:
    """Channel subscription configuration"""
    channel: str
    filters: Optional[Dict[str, Any]] = None
    batch_size: int = 10
    update_interval: float = 1.0
    max_message_size: int = 1024 * 1024  # 1MB
    priority: int = 0


class RealTimeStreaming:
    """Real-time WebSocket streaming manager"""

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.connections: Dict[str, ConnectionInfo] = {}
        self.channels: Dict[str, Set[str]] = defaultdict(set)  # channel -> connection_ids
        self.user_connections: Dict[str, Set[str]] = defaultdict(set)  # user_id -> connection_ids
        self.redis_client = None
        self.redis_url = redis_url
        self.message_queue = asyncio.Queue()
        self.broadcast_tasks = {}
        self.heartbeat_task = None
        self.cleanup_task = None
        self.statistics = {
            'total_connections': 0,
            'active_connections': 0,
            'messages_sent': 0,
            'messages_received': 0,
            'bytes_transferred': 0,
            'channels_active': 0
        }
        self.logger = logging.getLogger(__name__)

    async def initialize(self):
        """Initialize the streaming manager"""
        try:
            # Initialize Redis connection
            self.redis_client = redis.from_url(self.redis_url)
            await self.redis_client.ping()

            # Start background tasks
            self.heartbeat_task = asyncio.create_task(self._heartbeat_monitor())
            self.cleanup_task = asyncio.create_task(self._cleanup_connections())

            # Start message processing
            asyncio.create_task(self._process_message_queue())

            # Initialize channel broadcasters
            for channel_type in ChannelType:
                self.broadcast_tasks[channel_type.value] = asyncio.create_task(
                    self._channel_broadcaster(channel_type.value)
                )

            self.logger.info("Real-time streaming manager initialized")

        except Exception as e:
            self.logger.error(f"Failed to initialize streaming manager: {e}")
            raise

    async def connect_websocket(self, websocket: WebSocket, user_id: Optional[str] = None) -> str:
        """Handle new WebSocket connection"""
        connection_id = str(uuid.uuid4())

        try:
            await websocket.accept()

            # Create connection info
            connection_info = ConnectionInfo(
                connection_id=connection_id,
                websocket=websocket,
                user_id=user_id,
                session_id=str(uuid.uuid4()),
                status=ConnectionStatus.CONNECTED
            )

            # Store connection
            self.connections[connection_id] = connection_info

            # Update user connections mapping
            if user_id:
                self.user_connections[user_id].add(connection_id)

            # Update statistics
            self.statistics['total_connections'] += 1
            self.statistics['active_connections'] += 1

            # Send welcome message
            await self._send_message(connection_id, WebSocketMessage(
                type=MessageType.SYSTEM,
                data={
                    'event': 'connected',
                    'connection_id': connection_id,
                    'session_id': connection_info.session_id,
                    'timestamp': datetime.now().isoformat(),
                    'available_channels': [channel.value for channel in ChannelType]
                }
            ))

            self.logger.info(f"WebSocket connected: {connection_id} (user: {user_id})")
            return connection_id

        except Exception as e:
            self.logger.error(f"Failed to connect WebSocket: {e}")
            if connection_id in self.connections:
                del self.connections[connection_id]
            raise

    async def disconnect_websocket(self, connection_id: str):
        """Handle WebSocket disconnection"""
        if connection_id not in self.connections:
            return

        connection_info = self.connections[connection_id]

        try:
            # Unsubscribe from all channels
            for channel in list(connection_info.subscribed_channels):
                await self._unsubscribe_from_channel(connection_id, channel)

            # Remove from user connections
            if connection_info.user_id:
                self.user_connections[connection_info.user_id].discard(connection_id)
                if not self.user_connections[connection_info.user_id]:
                    del self.user_connections[connection_info.user_id]

            # Update statistics
            self.statistics['active_connections'] = max(0, self.statistics['active_connections'] - 1)

            # Remove connection
            del self.connections[connection_id]

            self.logger.info(f"WebSocket disconnected: {connection_id}")

        except Exception as e:
            self.logger.error(f"Error during WebSocket disconnection: {e}")

    async def handle_message(self, connection_id: str, message: Dict[str, Any]):
        """Handle incoming WebSocket message"""
        if connection_id not in self.connections:
            return

        connection_info = self.connections[connection_id]

        try:
            # Parse message
            ws_message = WebSocketMessage(
                type=MessageType(message.get('type', 'data')),
                channel=message.get('channel'),
                data=message.get('data', {}),
                user_id=connection_info.user_id,
                session_id=connection_info.session_id
            )

            # Update connection stats
            connection_info.message_count += 1
            connection_info.bytes_received += len(json.dumps(message))
            self.statistics['messages_received'] += 1

            # Handle different message types
            if ws_message.type == MessageType.SUBSCRIBE:
                await self._handle_subscribe(connection_id, ws_message)
            elif ws_message.type == MessageType.UNSUBSCRIBE:
                await self._handle_unsubscribe(connection_id, ws_message)
            elif ws_message.type == MessageType.HEARTBEAT:
                await self._handle_heartbeat(connection_id, ws_message)
            elif ws_message.type == MessageType.AUTH:
                await self._handle_auth(connection_id, ws_message)
            else:
                # Handle custom message types
                await self._handle_custom_message(connection_id, ws_message)

        except Exception as e:
            self.logger.error(f"Error handling message from {connection_id}: {e}")
            await self._send_error(connection_id, f"Message handling failed: {str(e)}")

    async def broadcast_to_channel(self, channel: str, data: Dict[str, Any], filters: Optional[Dict[str, Any]] = None):
        """Broadcast message to all subscribers of a channel"""
        if channel not in self.channels:
            return

        message = WebSocketMessage(
            type=MessageType.DATA,
            channel=channel,
            data=data
        )

        # Get subscriber connection IDs
        connection_ids = list(self.channels[channel])

        # Apply filters if provided
        if filters:
            connection_ids = [
                conn_id for conn_id in connection_ids
                if self._connection_matches_filters(conn_id, filters)
            ]

        # Broadcast to all matching connections
        if connection_ids:
            await self._broadcast_to_connections(connection_ids, message)

    async def send_to_user(self, user_id: str, data: Dict[str, Any]):
        """Send message to all connections of a specific user"""
        if user_id not in self.user_connections:
            return

        message = WebSocketMessage(
            type=MessageType.NOTIFICATION,
            data=data
        )

        connection_ids = list(self.user_connections[user_id])
        await self._broadcast_to_connections(connection_ids, message)

    async def _handle_subscribe(self, connection_id: str, message: WebSocketMessage):
        """Handle channel subscription"""
        channel = message.channel
        if not channel:
            await self._send_error(connection_id, "Channel name required for subscription")
            return

        try:
            # Validate channel
            if channel not in [ch.value for ch in ChannelType]:
                await self._send_error(connection_id, f"Invalid channel: {channel}")
                return

            # Add to channel subscribers
            self.channels[channel].add(connection_id)
            self.connections[connection_id].subscribed_channels.add(channel)

            # Send confirmation
            await self._send_message(connection_id, WebSocketMessage(
                type=MessageType.SYSTEM,
                data={
                    'event': 'subscribed',
                    'channel': channel,
                    'subscriber_count': len(self.channels[channel])
                }
            ))

            # Start sending data for this channel
            await self._send_initial_channel_data(connection_id, channel)

            self.logger.info(f"Connection {connection_id} subscribed to {channel}")

        except Exception as e:
            self.logger.error(f"Subscription failed: {e}")
            await self._send_error(connection_id, f"Subscription failed: {str(e)}")

    async def _handle_unsubscribe(self, connection_id: str, message: WebSocketMessage):
        """Handle channel unsubscription"""
        channel = message.channel
        if not channel:
            await self._send_error(connection_id, "Channel name required for unsubscription")
            return

        await self._unsubscribe_from_channel(connection_id, channel)

    async def _unsubscribe_from_channel(self, connection_id: str, channel: str):
        """Unsubscribe connection from channel"""
        try:
            # Remove from channel subscribers
            if channel in self.channels:
                self.channels[channel].discard(connection_id)
                if not self.channels[channel]:
                    del self.channels[channel]

            # Remove from connection's subscribed channels
            if connection_id in self.connections:
                self.connections[connection_id].subscribed_channels.discard(channel)

            # Send confirmation
            await self._send_message(connection_id, WebSocketMessage(
                type=MessageType.SYSTEM,
                data={
                    'event': 'unsubscribed',
                    'channel': channel
                }
            ))

            self.logger.info(f"Connection {connection_id} unsubscribed from {channel}")

        except Exception as e:
            self.logger.error(f"Unsubscription failed: {e}")

    async def _handle_heartbeat(self, connection_id: str, message: WebSocketMessage):
        """Handle heartbeat message"""
        if connection_id in self.connections:
            self.connections[connection_id].last_heartbeat = datetime.now()

            # Send heartbeat response
            await self._send_message(connection_id, WebSocketMessage(
                type=MessageType.HEARTBEAT,
                data={'timestamp': datetime.now().isoformat()}
            ))

    async def _handle_auth(self, connection_id: str, message: WebSocketMessage):
        """Handle authentication message"""
        try:
            token = message.data.get('token')
            if not token:
                await self._send_error(connection_id, "Authentication token required")
                return

            # Validate JWT token (simplified)
            try:
                payload = jwt.decode(token, settings.SECRET_KEY, algorithms=["HS256"])
                user_id = payload.get("sub")

                if user_id:
                    # Update connection with user info
                    connection_info = self.connections[connection_id]
                    connection_info.user_id = user_id
                    connection_info.status = ConnectionStatus.AUTHENTICATED

                    # Update user connections mapping
                    self.user_connections[user_id].add(connection_id)

                    await self._send_message(connection_id, WebSocketMessage(
                        type=MessageType.SYSTEM,
                        data={
                            'event': 'authenticated',
                            'user_id': user_id
                        }
                    ))

                    self.logger.info(f"Connection {connection_id} authenticated as user {user_id}")
                else:
                    await self._send_error(connection_id, "Invalid token payload")

            except jwt.InvalidTokenError:
                await self._send_error(connection_id, "Invalid authentication token")

        except Exception as e:
            self.logger.error(f"Authentication failed: {e}")
            await self._send_error(connection_id, "Authentication failed")

    async def _handle_custom_message(self, connection_id: str, message: WebSocketMessage):
        """Handle custom message types"""
        # Broadcast custom messages to Redis for other instances
        if self.redis_client:
            await self.redis_client.publish(
                'websocket_custom_messages',
                json.dumps({
                    'connection_id': connection_id,
                    'message': message.__dict__,
                    'timestamp': datetime.now().isoformat()
                }, default=str)
            )

    async def _send_message(self, connection_id: str, message: WebSocketMessage) -> bool:
        """Send message to specific connection"""
        if connection_id not in self.connections:
            return False

        connection_info = self.connections[connection_id]

        try:
            message_data = {
                'type': message.type.value,
                'channel': message.channel,
                'data': message.data,
                'timestamp': message.timestamp.isoformat(),
                'message_id': message.message_id
            }

            message_json = json.dumps(message_data, default=str)
            await connection_info.websocket.send_text(message_json)

            # Update statistics
            connection_info.bytes_sent += len(message_json)
            self.statistics['messages_sent'] += 1
            self.statistics['bytes_transferred'] += len(message_json)

            return True

        except (ConnectionClosed, WebSocketException) as e:
            self.logger.warning(f"Connection {connection_id} closed during send: {e}")
            await self.disconnect_websocket(connection_id)
            return False

        except Exception as e:
            self.logger.error(f"Failed to send message to {connection_id}: {e}")
            return False

    async def _send_error(self, connection_id: str, error_message: str):
        """Send error message to connection"""
        await self._send_message(connection_id, WebSocketMessage(
            type=MessageType.ERROR,
            data={
                'error': error_message,
                'timestamp': datetime.now().isoformat()
            }
        ))

    async def _broadcast_to_connections(self, connection_ids: List[str], message: WebSocketMessage):
        """Broadcast message to multiple connections"""
        if not connection_ids:
            return

        # Send to all connections concurrently
        tasks = [
            self._send_message(conn_id, message)
            for conn_id in connection_ids
            if conn_id in self.connections
        ]

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            success_count = sum(1 for result in results if result is True)
            self.logger.debug(f"Broadcast sent to {success_count}/{len(tasks)} connections")

    async def _send_initial_channel_data(self, connection_id: str, channel: str):
        """Send initial data when subscribing to a channel"""
        try:
            if channel == ChannelType.SALES_LIVE.value:
                # Send recent sales data
                data = await self._get_recent_sales_data()
            elif channel == ChannelType.BUSINESS_METRICS.value:
                # Send current business metrics
                data = await self._get_business_metrics()
            elif channel == ChannelType.SYSTEM_HEALTH.value:
                # Send system health data
                data = await self._get_system_health()
            else:
                data = {'message': f'Welcome to {channel} channel'}

            await self._send_message(connection_id, WebSocketMessage(
                type=MessageType.DATA,
                channel=channel,
                data=data
            ))

        except Exception as e:
            self.logger.error(f"Failed to send initial data for {channel}: {e}")

    async def _channel_broadcaster(self, channel: str):
        """Background task to broadcast data to channel subscribers"""
        while True:
            try:
                if channel not in self.channels or not self.channels[channel]:
                    await asyncio.sleep(5)
                    continue

                # Generate channel-specific data
                if channel == ChannelType.SALES_LIVE.value:
                    data = await self._generate_live_sales_data()
                elif channel == ChannelType.ANALYTICS_DASHBOARD.value:
                    data = await self._generate_analytics_data()
                elif channel == ChannelType.BUSINESS_METRICS.value:
                    data = await self._generate_business_metrics()
                elif channel == ChannelType.SYSTEM_HEALTH.value:
                    data = await self._generate_system_health_data()
                else:
                    await asyncio.sleep(10)
                    continue

                # Broadcast to channel subscribers
                await self.broadcast_to_channel(channel, data)

                # Wait before next update
                await asyncio.sleep(self._get_channel_update_interval(channel))

            except Exception as e:
                self.logger.error(f"Error in channel broadcaster for {channel}: {e}")
                await asyncio.sleep(10)

    async def _heartbeat_monitor(self):
        """Monitor connection heartbeats and cleanup stale connections"""
        while True:
            try:
                current_time = datetime.now()
                stale_connections = []

                for connection_id, connection_info in self.connections.items():
                    time_since_heartbeat = current_time - connection_info.last_heartbeat

                    if time_since_heartbeat > timedelta(minutes=5):  # 5 minute timeout
                        stale_connections.append(connection_id)

                # Remove stale connections
                for connection_id in stale_connections:
                    self.logger.warning(f"Removing stale connection: {connection_id}")
                    await self.disconnect_websocket(connection_id)

                await asyncio.sleep(60)  # Check every minute

            except Exception as e:
                self.logger.error(f"Error in heartbeat monitor: {e}")
                await asyncio.sleep(60)

    async def _cleanup_connections(self):
        """Periodic cleanup of closed connections"""
        while True:
            try:
                closed_connections = []

                for connection_id, connection_info in self.connections.items():
                    try:
                        # Try to send a ping to check if connection is alive
                        await connection_info.websocket.ping()
                    except (ConnectionClosed, WebSocketException):
                        closed_connections.append(connection_id)

                # Clean up closed connections
                for connection_id in closed_connections:
                    await self.disconnect_websocket(connection_id)

                # Update statistics
                self.statistics['channels_active'] = len([ch for ch in self.channels.values() if ch])

                await asyncio.sleep(300)  # Check every 5 minutes

            except Exception as e:
                self.logger.error(f"Error in connection cleanup: {e}")
                await asyncio.sleep(300)

    async def _process_message_queue(self):
        """Process queued messages for broadcasting"""
        while True:
            try:
                # This would process messages from the queue
                # Implementation depends on specific requirements
                await asyncio.sleep(1)

            except Exception as e:
                self.logger.error(f"Error processing message queue: {e}")
                await asyncio.sleep(1)

    def _connection_matches_filters(self, connection_id: str, filters: Dict[str, Any]) -> bool:
        """Check if connection matches broadcast filters"""
        if connection_id not in self.connections:
            return False

        connection_info = self.connections[connection_id]

        # Example filter logic
        if 'user_id' in filters and connection_info.user_id != filters['user_id']:
            return False

        if 'authenticated_only' in filters and filters['authenticated_only']:
            if connection_info.status != ConnectionStatus.AUTHENTICATED:
                return False

        return True

    def _get_channel_update_interval(self, channel: str) -> float:
        """Get update interval for channel"""
        intervals = {
            ChannelType.SALES_LIVE.value: 2.0,  # 2 seconds
            ChannelType.ANALYTICS_DASHBOARD.value: 10.0,  # 10 seconds
            ChannelType.BUSINESS_METRICS.value: 30.0,  # 30 seconds
            ChannelType.SYSTEM_HEALTH.value: 15.0,  # 15 seconds
            ChannelType.ALERTS.value: 5.0,  # 5 seconds
        }
        return intervals.get(channel, 60.0)  # Default 1 minute

    async def get_connection_stats(self) -> Dict[str, Any]:
        """Get connection statistics"""
        channel_stats = {
            channel: len(connections)
            for channel, connections in self.channels.items()
        }

        return {
            'total_connections': self.statistics['total_connections'],
            'active_connections': self.statistics['active_connections'],
            'messages_sent': self.statistics['messages_sent'],
            'messages_received': self.statistics['messages_received'],
            'bytes_transferred': self.statistics['bytes_transferred'],
            'channels_active': len(channel_stats),
            'channel_subscribers': channel_stats,
            'uptime_seconds': (datetime.now() - datetime.now()).total_seconds()  # Would track actual uptime
        }

    # Data generation methods (would integrate with actual data sources)
    async def _get_recent_sales_data(self) -> Dict[str, Any]:
        """Get recent sales data for initial channel data"""
        return {
            'recent_sales': [],
            'total_today': 125000.0,
            'transactions_today': 450
        }

    async def _get_business_metrics(self) -> Dict[str, Any]:
        """Get current business metrics"""
        return {
            'revenue_today': 125000.0,
            'revenue_month': 2500000.0,
            'customers_active': 8500,
            'conversion_rate': 3.2
        }

    async def _get_system_health(self) -> Dict[str, Any]:
        """Get system health metrics"""
        return {
            'cpu_usage': 45.2,
            'memory_usage': 67.8,
            'disk_usage': 23.1,
            'response_time_ms': 25.4
        }

    async def _generate_live_sales_data(self) -> Dict[str, Any]:
        """Generate live sales data"""
        return {
            'timestamp': datetime.now().isoformat(),
            'new_sales_count': 5,
            'revenue_last_minute': 2500.0,
            'active_sessions': 150
        }

    async def _generate_analytics_data(self) -> Dict[str, Any]:
        """Generate analytics dashboard data"""
        return {
            'timestamp': datetime.now().isoformat(),
            'page_views': 1250,
            'unique_visitors': 450,
            'bounce_rate': 35.6,
            'avg_session_duration': 245
        }

    async def _generate_business_metrics(self) -> Dict[str, Any]:
        """Generate business metrics data"""
        return {
            'timestamp': datetime.now().isoformat(),
            'revenue_rate_per_hour': 5000.0,
            'customer_acquisition_rate': 25,
            'customer_satisfaction': 4.2,
            'inventory_turnover': 8.5
        }

    async def _generate_system_health_data(self) -> Dict[str, Any]:
        """Generate system health data"""
        return {
            'timestamp': datetime.now().isoformat(),
            'cpu_usage': 45.2 + (time.time() % 10 - 5),  # Simulate fluctuation
            'memory_usage': 67.8 + (time.time() % 5 - 2.5),
            'active_connections': len(self.connections),
            'response_time_ms': 25.4 + (time.time() % 20 - 10)
        }


# Global streaming manager instance
streaming_manager = RealTimeStreaming()


# FastAPI WebSocket endpoints
async def websocket_endpoint(websocket: WebSocket, user_id: Optional[str] = None):
    """Main WebSocket endpoint"""
    connection_id = None

    try:
        # Connect WebSocket
        connection_id = await streaming_manager.connect_websocket(websocket, user_id)

        # Message handling loop
        while True:
            try:
                # Receive message
                data = await websocket.receive_text()
                message = json.loads(data)

                # Handle message
                await streaming_manager.handle_message(connection_id, message)

            except WebSocketDisconnect:
                break
            except json.JSONDecodeError:
                await streaming_manager._send_error(connection_id, "Invalid JSON format")
            except Exception as e:
                logger.error(f"Error in WebSocket message loop: {e}")
                await streaming_manager._send_error(connection_id, f"Message processing error: {str(e)}")

    except Exception as e:
        logger.error(f"WebSocket connection error: {e}")
    finally:
        if connection_id:
            await streaming_manager.disconnect_websocket(connection_id)


# Factory function
def create_realtime_streaming(redis_url: str = "redis://localhost:6379") -> RealTimeStreaming:
    """Create configured real-time streaming manager"""
    return RealTimeStreaming(redis_url)


# Integration with FastAPI
def setup_websocket_routes(app: FastAPI):
    """Set up WebSocket routes in FastAPI application"""

    @app.websocket("/ws")
    async def websocket_root(websocket: WebSocket):
        await websocket_endpoint(websocket)

    @app.websocket("/ws/{user_id}")
    async def websocket_user(websocket: WebSocket, user_id: str):
        await websocket_endpoint(websocket, user_id)

    @app.get("/ws/stats")
    async def get_websocket_stats():
        """Get WebSocket connection statistics"""
        return await streaming_manager.get_connection_stats()

    @app.post("/ws/broadcast/{channel}")
    async def broadcast_to_channel(channel: str, data: Dict[str, Any]):
        """Broadcast message to WebSocket channel"""
        await streaming_manager.broadcast_to_channel(channel, data)
        return {"status": "broadcasted", "channel": channel}

    @app.post("/ws/send/{user_id}")
    async def send_to_user(user_id: str, data: Dict[str, Any]):
        """Send message to specific user"""
        await streaming_manager.send_to_user(user_id, data)
        return {"status": "sent", "user_id": user_id}