"""
Service-to-Service Communication Patterns
Implements various communication patterns for microservices architecture.
"""

from __future__ import annotations

import asyncio
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

import aiohttp
import orjson
import redis.asyncio as redis

from core.config import get_settings
from core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


class MessageType(str, Enum):
    """Types of inter-service messages"""

    COMMAND = "command"
    EVENT = "event"
    QUERY = "query"
    RESPONSE = "response"
    HEARTBEAT = "heartbeat"
    ERROR = "error"


class CommunicationPattern(str, Enum):
    """Communication pattern types"""

    REQUEST_RESPONSE = "request_response"
    PUBLISH_SUBSCRIBE = "publish_subscribe"
    MESSAGE_QUEUE = "message_queue"
    EVENT_STREAMING = "event_streaming"
    RPC = "rpc"


class ServiceStatus(str, Enum):
    """Service status in the registry"""

    ONLINE = "online"
    OFFLINE = "offline"
    DEGRADED = "degraded"
    MAINTENANCE = "maintenance"


@dataclass
class ServiceEndpoint:
    """Service endpoint information"""

    service_name: str
    host: str
    port: int
    protocol: str = "http"
    health_endpoint: str = "/health"
    version: str = "1.0.0"
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def base_url(self) -> str:
        """Get base URL for the service"""
        return f"{self.protocol}://{self.host}:{self.port}"

    @property
    def health_url(self) -> str:
        """Get health check URL"""
        return f"{self.base_url}{self.health_endpoint}"


@dataclass
class Message:
    """Inter-service message"""

    id: str
    type: MessageType
    source_service: str
    target_service: str
    payload: dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    correlation_id: str | None = None
    reply_to: str | None = None
    ttl: int | None = None  # Time to live in seconds
    headers: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert message to dictionary"""
        return {
            "id": self.id,
            "type": self.type.value,
            "source_service": self.source_service,
            "target_service": self.target_service,
            "payload": self.payload,
            "timestamp": self.timestamp.isoformat(),
            "correlation_id": self.correlation_id,
            "reply_to": self.reply_to,
            "ttl": self.ttl,
            "headers": self.headers,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Message:
        """Create message from dictionary"""
        return cls(
            id=data["id"],
            type=MessageType(data["type"]),
            source_service=data["source_service"],
            target_service=data["target_service"],
            payload=data["payload"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            correlation_id=data.get("correlation_id"),
            reply_to=data.get("reply_to"),
            ttl=data.get("ttl"),
            headers=data.get("headers", {}),
        )


class ServiceDiscovery:
    """Service discovery and registration"""

    def __init__(self, redis_client: redis.Redis | None = None):
        self.redis_client = redis_client or self._create_redis_client()
        self._services: dict[str, ServiceEndpoint] = {}
        self._health_check_interval = 30  # seconds
        self._health_check_task: asyncio.Task | None = None
        self._service_ttl = 60  # Service registration TTL in seconds

    def _create_redis_client(self) -> redis.Redis:
        """Create Redis client for service registry"""
        try:
            redis_url = getattr(settings, "redis_url", "redis://localhost:6379/2")
            return redis.from_url(
                redis_url,
                encoding="utf-8",
                decode_responses=False,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                max_connections=10,
            )
        except Exception as e:
            logger.warning(f"Failed to create Redis client: {e}")
            return None

    async def register_service(self, endpoint: ServiceEndpoint) -> bool:
        """Register a service endpoint"""
        try:
            self._services[endpoint.service_name] = endpoint

            if self.redis_client:
                # Store in Redis with TTL
                service_data = {
                    "service_name": endpoint.service_name,
                    "host": endpoint.host,
                    "port": endpoint.port,
                    "protocol": endpoint.protocol,
                    "health_endpoint": endpoint.health_endpoint,
                    "version": endpoint.version,
                    "metadata": endpoint.metadata,
                    "registered_at": datetime.utcnow().isoformat(),
                    "status": ServiceStatus.ONLINE.value,
                }

                await self.redis_client.setex(
                    f"service:{endpoint.service_name}",
                    self._service_ttl,
                    orjson.dumps(service_data),
                )

            logger.info(f"Registered service: {endpoint.service_name} at {endpoint.base_url}")
            return True

        except Exception as e:
            logger.error(f"Failed to register service {endpoint.service_name}: {e}")
            return False

    async def deregister_service(self, service_name: str) -> bool:
        """Deregister a service"""
        try:
            if service_name in self._services:
                del self._services[service_name]

            if self.redis_client:
                await self.redis_client.delete(f"service:{service_name}")

            logger.info(f"Deregistered service: {service_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to deregister service {service_name}: {e}")
            return False

    async def discover_service(self, service_name: str) -> ServiceEndpoint | None:
        """Discover a service endpoint"""
        try:
            # Check local cache first
            if service_name in self._services:
                return self._services[service_name]

            # Check Redis
            if self.redis_client:
                service_data = await self.redis_client.get(f"service:{service_name}")
                if service_data:
                    data = orjson.loads(service_data)
                    endpoint = ServiceEndpoint(
                        service_name=data["service_name"],
                        host=data["host"],
                        port=data["port"],
                        protocol=data.get("protocol", "http"),
                        health_endpoint=data.get("health_endpoint", "/health"),
                        version=data.get("version", "1.0.0"),
                        metadata=data.get("metadata", {}),
                    )

                    # Cache locally
                    self._services[service_name] = endpoint
                    return endpoint

            return None

        except Exception as e:
            logger.error(f"Failed to discover service {service_name}: {e}")
            return None

    async def list_services(self) -> list[ServiceEndpoint]:
        """List all registered services"""
        services = []

        try:
            if self.redis_client:
                # Get all service keys
                keys = await self.redis_client.keys("service:*")
                for key in keys:
                    service_data = await self.redis_client.get(key)
                    if service_data:
                        data = orjson.loads(service_data)
                        endpoint = ServiceEndpoint(
                            service_name=data["service_name"],
                            host=data["host"],
                            port=data["port"],
                            protocol=data.get("protocol", "http"),
                            health_endpoint=data.get("health_endpoint", "/health"),
                            version=data.get("version", "1.0.0"),
                            metadata=data.get("metadata", {}),
                        )
                        services.append(endpoint)
            else:
                # Fallback to local cache
                services = list(self._services.values())

            return services

        except Exception as e:
            logger.error(f"Failed to list services: {e}")
            return []

    async def health_check_service(self, endpoint: ServiceEndpoint) -> bool:
        """Check if a service is healthy"""
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
                async with session.get(endpoint.health_url) as response:
                    return response.status == 200
        except Exception as e:
            logger.warning(f"Health check failed for {endpoint.service_name}: {e}")
            return False

    async def start_health_monitoring(self) -> None:
        """Start background health monitoring"""
        if self._health_check_task:
            return

        self._health_check_task = asyncio.create_task(self._health_check_loop())
        logger.info("Started service health monitoring")

    async def stop_health_monitoring(self) -> None:
        """Stop background health monitoring"""
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
            self._health_check_task = None
            logger.info("Stopped service health monitoring")

    async def _health_check_loop(self) -> None:
        """Background health check loop"""
        while True:
            try:
                await asyncio.sleep(self._health_check_interval)

                services = await self.list_services()
                for service in services:
                    is_healthy = await self.health_check_service(service)

                    # Update service status in Redis
                    if self.redis_client:
                        status = ServiceStatus.ONLINE if is_healthy else ServiceStatus.OFFLINE
                        await self.redis_client.hset(
                            f"service:{service.service_name}",
                            "status",
                            status.value,
                            "last_health_check",
                            datetime.utcnow().isoformat(),
                        )

                    if not is_healthy:
                        logger.warning(f"Service {service.service_name} is unhealthy")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check loop error: {e}")


class MessageBroker(ABC):
    """Abstract message broker interface"""

    @abstractmethod
    async def publish(self, message: Message) -> bool:
        """Publish a message"""
        pass

    @abstractmethod
    async def subscribe(self, service_name: str, callback: Callable[[Message], None]) -> None:
        """Subscribe to messages for a service"""
        pass

    @abstractmethod
    async def send_direct(self, message: Message) -> bool:
        """Send message directly to a service"""
        pass


class RedisMessageBroker(MessageBroker):
    """Redis-based message broker"""

    def __init__(self, redis_client: redis.Redis | None = None):
        self.redis_client = redis_client or self._create_redis_client()
        self._subscribers: dict[str, list[Callable]] = {}
        self._pubsub_task: asyncio.Task | None = None

    def _create_redis_client(self) -> redis.Redis:
        """Create Redis client for message broker"""
        try:
            redis_url = getattr(settings, "redis_url", "redis://localhost:6379/3")
            return redis.from_url(
                redis_url,
                encoding="utf-8",
                decode_responses=False,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                max_connections=20,
            )
        except Exception as e:
            logger.warning(f"Failed to create Redis client: {e}")
            return None

    async def publish(self, message: Message) -> bool:
        """Publish message to a channel"""
        if not self.redis_client:
            return False

        try:
            channel = f"service:{message.target_service}"
            message_data = orjson.dumps(message.to_dict())

            await self.redis_client.publish(channel, message_data)
            logger.debug(f"Published message {message.id} to {channel}")
            return True

        except Exception as e:
            logger.error(f"Failed to publish message {message.id}: {e}")
            return False

    async def send_direct(self, message: Message) -> bool:
        """Send message directly using Redis queue"""
        if not self.redis_client:
            return False

        try:
            queue_name = f"queue:{message.target_service}"
            message_data = orjson.dumps(message.to_dict())

            # Add TTL if specified
            if message.ttl:
                await self.redis_client.lpush(queue_name, message_data)
                await self.redis_client.expire(queue_name, message.ttl)
            else:
                await self.redis_client.lpush(queue_name, message_data)

            logger.debug(f"Sent direct message {message.id} to {queue_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to send direct message {message.id}: {e}")
            return False

    async def subscribe(self, service_name: str, callback: Callable[[Message], None]) -> None:
        """Subscribe to messages for a service"""
        if not self.redis_client:
            return

        if service_name not in self._subscribers:
            self._subscribers[service_name] = []

        self._subscribers[service_name].append(callback)

        # Start pubsub task if not already running
        if not self._pubsub_task:
            self._pubsub_task = asyncio.create_task(self._pubsub_loop())

    async def _pubsub_loop(self) -> None:
        """Background pubsub message processing loop"""
        if not self.redis_client:
            return

        pubsub = self.redis_client.pubsub()

        try:
            # Subscribe to all service channels
            for service_name in self._subscribers:
                await pubsub.subscribe(f"service:{service_name}")

            async for message in pubsub.listen():
                if message["type"] == "message":
                    try:
                        channel = message["channel"].decode()
                        service_name = channel.replace("service:", "")

                        # Parse message
                        msg_data = orjson.loads(message["data"])
                        msg = Message.from_dict(msg_data)

                        # Call all subscribers for this service
                        callbacks = self._subscribers.get(service_name, [])
                        for callback in callbacks:
                            try:
                                await callback(msg) if asyncio.iscoroutinefunction(
                                    callback
                                ) else callback(msg)
                            except Exception as e:
                                logger.error(f"Callback error for message {msg.id}: {e}")

                    except Exception as e:
                        logger.error(f"Error processing pubsub message: {e}")

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Pubsub loop error: {e}")
        finally:
            await pubsub.close()


class ServiceCommunicator:
    """High-level service communication interface"""

    def __init__(
        self, service_name: str, service_discovery: ServiceDiscovery, message_broker: MessageBroker
    ):
        self.service_name = service_name
        self.discovery = service_discovery
        self.broker = message_broker
        self._pending_requests: dict[str, asyncio.Future] = {}
        self._request_timeout = 30  # seconds

        # Subscribe to messages for this service
        asyncio.create_task(self.broker.subscribe(self.service_name, self._handle_message))

    async def _handle_message(self, message: Message) -> None:
        """Handle incoming messages"""
        try:
            if message.type == MessageType.RESPONSE:
                # Handle response to a previous request
                correlation_id = message.correlation_id
                if correlation_id in self._pending_requests:
                    future = self._pending_requests.pop(correlation_id)
                    if not future.done():
                        future.set_result(message)

            elif message.type == MessageType.COMMAND:
                # Handle command - implement command handling logic
                logger.info(f"Received command: {message.payload}")
                # Send acknowledgment
                response = await self._create_response_message(message, {"status": "acknowledged"})
                await self.broker.publish(response)

            elif message.type == MessageType.EVENT:
                # Handle event - implement event handling logic
                logger.info(f"Received event: {message.payload}")

            elif message.type == MessageType.QUERY:
                # Handle query - implement query handling logic
                logger.info(f"Received query: {message.payload}")
                # Send response with data
                response_data = {
                    "result": "query_result",
                    "timestamp": datetime.utcnow().isoformat(),
                }
                response = await self._create_response_message(message, response_data)
                await self.broker.publish(response)

        except Exception as e:
            logger.error(f"Error handling message {message.id}: {e}")

    async def _create_response_message(self, original: Message, payload: dict[str, Any]) -> Message:
        """Create response message"""
        return Message(
            id=str(uuid.uuid4()),
            type=MessageType.RESPONSE,
            source_service=self.service_name,
            target_service=original.source_service,
            payload=payload,
            correlation_id=original.id,
            headers={"response_to": original.id},
        )

    async def send_command(
        self,
        target_service: str,
        command: str,
        parameters: dict[str, Any] = None,
        wait_for_response: bool = True,
    ) -> Message | None:
        """Send command to another service"""
        message = Message(
            id=str(uuid.uuid4()),
            type=MessageType.COMMAND,
            source_service=self.service_name,
            target_service=target_service,
            payload={"command": command, "parameters": parameters or {}},
        )

        if wait_for_response:
            # Set up future for response
            future = asyncio.Future()
            self._pending_requests[message.id] = future

            # Send message
            await self.broker.publish(message)

            try:
                # Wait for response with timeout
                response = await asyncio.wait_for(future, timeout=self._request_timeout)
                return response
            except asyncio.TimeoutError:
                # Clean up pending request
                self._pending_requests.pop(message.id, None)
                logger.warning(f"Command timeout for message {message.id}")
                return None
        else:
            await self.broker.publish(message)
            return None

    async def send_query(
        self, target_service: str, query: str, parameters: dict[str, Any] = None
    ) -> Message | None:
        """Send query to another service and wait for response"""
        message = Message(
            id=str(uuid.uuid4()),
            type=MessageType.QUERY,
            source_service=self.service_name,
            target_service=target_service,
            payload={"query": query, "parameters": parameters or {}},
        )

        # Set up future for response
        future = asyncio.Future()
        self._pending_requests[message.id] = future

        # Send message
        await self.broker.publish(message)

        try:
            # Wait for response with timeout
            response = await asyncio.wait_for(future, timeout=self._request_timeout)
            return response
        except asyncio.TimeoutError:
            # Clean up pending request
            self._pending_requests.pop(message.id, None)
            logger.warning(f"Query timeout for message {message.id}")
            return None

    async def publish_event(self, event_name: str, data: dict[str, Any]) -> bool:
        """Publish event to all interested services"""
        message = Message(
            id=str(uuid.uuid4()),
            type=MessageType.EVENT,
            source_service=self.service_name,
            target_service="*",  # Broadcast
            payload={"event": event_name, "data": data},
        )

        return await self.broker.publish(message)

    async def send_direct_message(
        self,
        target_service: str,
        payload: dict[str, Any],
        message_type: MessageType = MessageType.COMMAND,
    ) -> bool:
        """Send direct message using message queue"""
        message = Message(
            id=str(uuid.uuid4()),
            type=message_type,
            source_service=self.service_name,
            target_service=target_service,
            payload=payload,
        )

        return await self.broker.send_direct(message)

    async def http_request(
        self,
        target_service: str,
        method: str,
        endpoint: str,
        data: dict[str, Any] = None,
        headers: dict[str, str] = None,
    ) -> dict[str, Any] | None:
        """Make HTTP request to another service"""
        service_endpoint = await self.discovery.discover_service(target_service)
        if not service_endpoint:
            logger.error(f"Service {target_service} not found")
            return None

        url = f"{service_endpoint.base_url}{endpoint}"

        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                request_headers = headers or {}
                request_headers["X-Source-Service"] = self.service_name
                request_headers["X-Request-ID"] = str(uuid.uuid4())

                if method.upper() == "GET":
                    async with session.get(url, headers=request_headers, params=data) as response:
                        if response.status == 200:
                            return await response.json()
                        else:
                            logger.error(f"HTTP request failed: {response.status}")
                            return None

                elif method.upper() == "POST":
                    async with session.post(url, headers=request_headers, json=data) as response:
                        if response.status in [200, 201]:
                            return await response.json()
                        else:
                            logger.error(f"HTTP request failed: {response.status}")
                            return None

                elif method.upper() == "PUT":
                    async with session.put(url, headers=request_headers, json=data) as response:
                        if response.status == 200:
                            return await response.json()
                        else:
                            logger.error(f"HTTP request failed: {response.status}")
                            return None

                elif method.upper() == "DELETE":
                    async with session.delete(url, headers=request_headers) as response:
                        if response.status in [200, 204]:
                            return {"success": True}
                        else:
                            logger.error(f"HTTP request failed: {response.status}")
                            return None

        except Exception as e:
            logger.error(f"HTTP request error to {target_service}: {e}")
            return None


class ServiceCommunicationManager:
    """Manages service communication patterns and orchestration"""

    def __init__(self):
        self.discovery = ServiceDiscovery()
        self.broker = RedisMessageBroker()
        self._communicators: dict[str, ServiceCommunicator] = {}
        self._is_running = False

    async def start(self) -> None:
        """Start the communication manager"""
        if self._is_running:
            return

        await self.discovery.start_health_monitoring()
        self._is_running = True
        logger.info("Service Communication Manager started")

    async def stop(self) -> None:
        """Stop the communication manager"""
        if not self._is_running:
            return

        await self.discovery.stop_health_monitoring()
        self._is_running = False
        logger.info("Service Communication Manager stopped")

    def get_communicator(self, service_name: str) -> ServiceCommunicator:
        """Get or create service communicator"""
        if service_name not in self._communicators:
            self._communicators[service_name] = ServiceCommunicator(
                service_name, self.discovery, self.broker
            )
        return self._communicators[service_name]

    async def register_service(
        self,
        service_name: str,
        host: str,
        port: int,
        protocol: str = "http",
        health_endpoint: str = "/health",
        metadata: dict[str, Any] = None,
    ) -> bool:
        """Register a service"""
        endpoint = ServiceEndpoint(
            service_name=service_name,
            host=host,
            port=port,
            protocol=protocol,
            health_endpoint=health_endpoint,
            metadata=metadata or {},
        )

        return await self.discovery.register_service(endpoint)


# Global communication manager instance
_communication_manager: ServiceCommunicationManager | None = None


def get_communication_manager() -> ServiceCommunicationManager:
    """Get global communication manager"""
    global _communication_manager
    if _communication_manager is None:
        _communication_manager = ServiceCommunicationManager()
    return _communication_manager


# Convenience functions
async def send_service_command(
    source_service: str,
    target_service: str,
    command: str,
    parameters: dict[str, Any] = None,
    wait_for_response: bool = True,
) -> Message | None:
    """Send command between services"""
    manager = get_communication_manager()
    communicator = manager.get_communicator(source_service)
    return await communicator.send_command(target_service, command, parameters, wait_for_response)


async def query_service(
    source_service: str, target_service: str, query: str, parameters: dict[str, Any] = None
) -> Message | None:
    """Query another service"""
    manager = get_communication_manager()
    communicator = manager.get_communicator(source_service)
    return await communicator.send_query(target_service, query, parameters)


async def publish_service_event(source_service: str, event_name: str, data: dict[str, Any]) -> bool:
    """Publish event from a service"""
    manager = get_communication_manager()
    communicator = manager.get_communicator(source_service)
    return await communicator.publish_event(event_name, data)
