"""
SAGA Pattern Implementation for Distributed Transactions
Handles distributed transactions across microservices with compensation
"""
import asyncio
import json
import time
import uuid
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, Awaitable

from core.logging import get_logger
from messaging.rabbitmq_manager import RabbitMQManager, TaskMessage, MessagePriority
from streaming.kafka_manager import KafkaManager, StreamingTopic

logger = get_logger(__name__)


class SagaStatus(Enum):
    """SAGA transaction status"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"


class StepStatus(Enum):
    """Individual step status"""
    PENDING = "pending"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"


@dataclass
class SagaStep:
    """Individual step in a SAGA transaction"""
    step_id: str
    service_name: str
    action: str
    payload: Dict[str, Any]
    compensation_action: str
    compensation_payload: Optional[Dict[str, Any]] = None
    timeout_seconds: int = 30
    retry_attempts: int = 3
    status: StepStatus = StepStatus.PENDING
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    executed_at: Optional[float] = None
    completed_at: Optional[float] = None
    
    def __post_init__(self):
        if self.compensation_payload is None:
            self.compensation_payload = {}


@dataclass
class SagaTransaction:
    """SAGA transaction definition"""
    saga_id: str
    transaction_type: str
    steps: List[SagaStep]
    status: SagaStatus = SagaStatus.PENDING
    metadata: Optional[Dict[str, Any]] = None
    created_at: float = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    timeout_seconds: int = 300  # 5 minutes default
    max_retry_attempts: int = 3
    correlation_id: Optional[str] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = time.time()
        if self.metadata is None:
            self.metadata = {}
        if self.correlation_id is None:
            self.correlation_id = str(uuid.uuid4())


class SagaOrchestrator:
    """Orchestrator for SAGA pattern distributed transactions"""
    
    def __init__(self):
        self.rabbitmq_manager = RabbitMQManager()
        self.kafka_manager = KafkaManager()
        self.active_sagas: Dict[str, SagaTransaction] = {}
        self.step_handlers: Dict[str, Callable] = {}
        self.compensation_handlers: Dict[str, Callable] = {}
        
        # Start background processing
        self._processing_task = None
        self._start_processing()
    
    def _start_processing(self):
        """Start background SAGA processing"""
        if self._processing_task is None or self._processing_task.done():
            self._processing_task = asyncio.create_task(self._process_sagas())
    
    async def _process_sagas(self):
        """Background task to process pending SAGAs"""
        while True:
            try:
                pending_sagas = [
                    saga for saga in self.active_sagas.values()
                    if saga.status in [SagaStatus.PENDING, SagaStatus.IN_PROGRESS, SagaStatus.COMPENSATING]
                ]
                
                for saga in pending_sagas:
                    await self._process_saga(saga)
                
                # Check for timeouts
                await self._check_saga_timeouts()
                
                await asyncio.sleep(1)  # Process every second
                
            except Exception as e:
                logger.error(f"SAGA processing error: {e}")
                await asyncio.sleep(5)
    
    def register_step_handler(self, service_name: str, action: str, handler: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]):
        """Register handler for a specific step"""
        key = f"{service_name}:{action}"
        self.step_handlers[key] = handler
    
    def register_compensation_handler(self, service_name: str, action: str, handler: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]):
        """Register compensation handler"""
        key = f"{service_name}:{action}"
        self.compensation_handlers[key] = handler
    
    async def start_saga(self, saga: SagaTransaction) -> str:
        """Start a new SAGA transaction"""
        logger.info(f"Starting SAGA: {saga.saga_id} ({saga.transaction_type})")
        
        saga.status = SagaStatus.PENDING
        saga.started_at = time.time()
        
        # Store active SAGA
        self.active_sagas[saga.saga_id] = saga
        
        # Publish SAGA started event
        await self._publish_saga_event(saga, "saga_started")
        
        return saga.saga_id
    
    async def _process_saga(self, saga: SagaTransaction):
        """Process a SAGA transaction"""
        try:
            if saga.status == SagaStatus.PENDING:
                saga.status = SagaStatus.IN_PROGRESS
                await self._publish_saga_event(saga, "saga_in_progress")
            
            if saga.status == SagaStatus.IN_PROGRESS:
                await self._execute_forward_steps(saga)
            elif saga.status == SagaStatus.COMPENSATING:
                await self._execute_compensation_steps(saga)
                
        except Exception as e:
            logger.error(f"Error processing SAGA {saga.saga_id}: {e}")
            await self._fail_saga(saga, str(e))
    
    async def _execute_forward_steps(self, saga: SagaTransaction):
        """Execute forward steps of SAGA"""
        for step in saga.steps:
            if step.status == StepStatus.PENDING:
                await self._execute_step(saga, step)
                
                if step.status == StepStatus.FAILED:
                    # Start compensation
                    await self._start_compensation(saga)
                    return
                elif step.status != StepStatus.COMPLETED:
                    # Step is still executing, wait for completion
                    return
        
        # All steps completed successfully
        await self._complete_saga(saga)
    
    async def _execute_compensation_steps(self, saga: SagaTransaction):
        """Execute compensation steps in reverse order"""
        completed_steps = [step for step in reversed(saga.steps) if step.status == StepStatus.COMPLETED]
        
        for step in completed_steps:
            if step.status != StepStatus.COMPENSATED:
                await self._compensate_step(saga, step)
                
                if step.status == StepStatus.FAILED:
                    # Compensation failed - manual intervention required
                    logger.error(f"Compensation failed for step {step.step_id} in SAGA {saga.saga_id}")
                    await self._publish_saga_event(saga, "compensation_failed", {
                        "step_id": step.step_id,
                        "error": step.error
                    })
                    return
        
        # All compensations completed
        saga.status = SagaStatus.COMPENSATED
        saga.completed_at = time.time()
        await self._publish_saga_event(saga, "saga_compensated")
        
        # Remove from active SAGAs
        self.active_sagas.pop(saga.saga_id, None)
    
    async def _execute_step(self, saga: SagaTransaction, step: SagaStep):
        """Execute a single SAGA step"""
        logger.info(f"Executing step {step.step_id} for SAGA {saga.saga_id}")
        
        step.status = StepStatus.EXECUTING
        step.executed_at = time.time()
        
        handler_key = f"{step.service_name}:{step.action}"
        
        try:
            if handler_key in self.step_handlers:
                # Execute locally registered handler
                result = await self.step_handlers[handler_key](step.payload)
                step.result = result
                step.status = StepStatus.COMPLETED
                step.completed_at = time.time()
                
                await self._publish_saga_event(saga, "step_completed", {
                    "step_id": step.step_id,
                    "result": result
                })
            else:
                # Send message to service via RabbitMQ
                await self._send_step_message(saga, step)
                # Status will be updated when response is received
                
        except Exception as e:
            step.error = str(e)
            step.status = StepStatus.FAILED
            
            logger.error(f"Step {step.step_id} failed: {e}")
            await self._publish_saga_event(saga, "step_failed", {
                "step_id": step.step_id,
                "error": str(e)
            })
    
    async def _compensate_step(self, saga: SagaTransaction, step: SagaStep):
        """Execute compensation for a step"""
        logger.info(f"Compensating step {step.step_id} for SAGA {saga.saga_id}")
        
        step.status = StepStatus.COMPENSATING
        
        handler_key = f"{step.service_name}:{step.compensation_action}"
        
        try:
            if handler_key in self.compensation_handlers:
                # Execute locally registered compensation handler
                result = await self.compensation_handlers[handler_key](step.compensation_payload)
                step.status = StepStatus.COMPENSATED
                
                await self._publish_saga_event(saga, "step_compensated", {
                    "step_id": step.step_id,
                    "result": result
                })
            else:
                # Send compensation message to service
                await self._send_compensation_message(saga, step)
                
        except Exception as e:
            step.error = str(e)
            step.status = StepStatus.FAILED
            
            logger.error(f"Compensation for step {step.step_id} failed: {e}")
    
    async def _send_step_message(self, saga: SagaTransaction, step: SagaStep):
        """Send step execution message to service"""
        message = TaskMessage(
            task_id=f"{saga.saga_id}:{step.step_id}",
            task_name=f"saga_step:{step.action}",
            payload={
                "saga_id": saga.saga_id,
                "step_id": step.step_id,
                "action": step.action,
                "data": step.payload
            },
            priority=MessagePriority.HIGH,
            correlation_id=saga.correlation_id,
            reply_to="saga_responses"
        )
        
        await self.rabbitmq_manager.publish_task_async(
            message,
            queue_name=f"saga_{step.service_name}",
            routing_key=step.action
        )
    
    async def _send_compensation_message(self, saga: SagaTransaction, step: SagaStep):
        """Send compensation message to service"""
        message = TaskMessage(
            task_id=f"{saga.saga_id}:{step.step_id}:compensation",
            task_name=f"saga_compensation:{step.compensation_action}",
            payload={
                "saga_id": saga.saga_id,
                "step_id": step.step_id,
                "action": step.compensation_action,
                "data": step.compensation_payload,
                "original_result": step.result
            },
            priority=MessagePriority.CRITICAL,
            correlation_id=saga.correlation_id,
            reply_to="saga_responses"
        )
        
        await self.rabbitmq_manager.publish_task_async(
            message,
            queue_name=f"saga_{step.service_name}",
            routing_key=step.compensation_action
        )
    
    async def handle_step_response(self, saga_id: str, step_id: str, success: bool, result: Dict[str, Any] = None, error: str = None):
        """Handle response from a SAGA step"""
        saga = self.active_sagas.get(saga_id)
        if not saga:
            logger.warning(f"Received response for unknown SAGA: {saga_id}")
            return
        
        # Find the step
        step = next((s for s in saga.steps if s.step_id == step_id), None)
        if not step:
            logger.warning(f"Received response for unknown step: {step_id}")
            return
        
        if success:
            step.status = StepStatus.COMPLETED
            step.result = result
            step.completed_at = time.time()
            
            await self._publish_saga_event(saga, "step_completed", {
                "step_id": step_id,
                "result": result
            })
        else:
            step.status = StepStatus.FAILED
            step.error = error
            
            await self._publish_saga_event(saga, "step_failed", {
                "step_id": step_id,
                "error": error
            })
            
            # Start compensation
            await self._start_compensation(saga)
    
    async def _start_compensation(self, saga: SagaTransaction):
        """Start compensation process for failed SAGA"""
        logger.info(f"Starting compensation for SAGA: {saga.saga_id}")
        
        saga.status = SagaStatus.COMPENSATING
        await self._publish_saga_event(saga, "saga_compensating")
    
    async def _complete_saga(self, saga: SagaTransaction):
        """Complete a successful SAGA"""
        logger.info(f"SAGA completed successfully: {saga.saga_id}")
        
        saga.status = SagaStatus.COMPLETED
        saga.completed_at = time.time()
        
        await self._publish_saga_event(saga, "saga_completed", {
            "total_steps": len(saga.steps),
            "duration_ms": (saga.completed_at - saga.started_at) * 1000
        })
        
        # Remove from active SAGAs
        self.active_sagas.pop(saga.saga_id, None)
    
    async def _fail_saga(self, saga: SagaTransaction, error: str):
        """Mark SAGA as failed"""
        logger.error(f"SAGA failed: {saga.saga_id} - {error}")
        
        saga.status = SagaStatus.FAILED
        saga.completed_at = time.time()
        
        await self._publish_saga_event(saga, "saga_failed", {
            "error": error,
            "completed_steps": sum(1 for s in saga.steps if s.status == StepStatus.COMPLETED)
        })
        
        # Start compensation for completed steps
        await self._start_compensation(saga)
    
    async def _check_saga_timeouts(self):
        """Check for timed out SAGAs"""
        current_time = time.time()
        
        for saga in list(self.active_sagas.values()):
            if saga.started_at and (current_time - saga.started_at) > saga.timeout_seconds:
                await self._fail_saga(saga, "SAGA timeout")
    
    async def _publish_saga_event(self, saga: SagaTransaction, event_type: str, extra_data: Dict[str, Any] = None):
        """Publish SAGA event to Kafka"""
        try:
            event_data = {
                "saga_id": saga.saga_id,
                "transaction_type": saga.transaction_type,
                "status": saga.status.value,
                "event_type": event_type,
                "timestamp": time.time(),
                "correlation_id": saga.correlation_id
            }
            
            if extra_data:
                event_data.update(extra_data)
            
            self.kafka_manager.produce_saga_event(
                saga_id=saga.saga_id,
                transaction_type=saga.transaction_type,
                event_type=event_type,
                event_data=event_data
            )
            
        except Exception as e:
            logger.error(f"Failed to publish SAGA event: {e}")
    
    async def get_saga_status(self, saga_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a SAGA transaction"""
        saga = self.active_sagas.get(saga_id)
        if saga:
            return {
                "saga_id": saga.saga_id,
                "transaction_type": saga.transaction_type,
                "status": saga.status.value,
                "steps": [
                    {
                        "step_id": step.step_id,
                        "service_name": step.service_name,
                        "action": step.action,
                        "status": step.status.value,
                        "error": step.error
                    }
                    for step in saga.steps
                ],
                "created_at": saga.created_at,
                "started_at": saga.started_at,
                "completed_at": saga.completed_at,
                "metadata": saga.metadata
            }
        return None
    
    async def get_all_active_sagas(self) -> List[Dict[str, Any]]:
        """Get all active SAGA transactions"""
        return [
            {
                "saga_id": saga.saga_id,
                "transaction_type": saga.transaction_type,
                "status": saga.status.value,
                "step_count": len(saga.steps),
                "completed_steps": sum(1 for s in saga.steps if s.status == StepStatus.COMPLETED),
                "started_at": saga.started_at,
                "duration": time.time() - saga.started_at if saga.started_at else 0
            }
            for saga in self.active_sagas.values()
        ]
    
    async def cancel_saga(self, saga_id: str) -> bool:
        """Cancel a running SAGA"""
        saga = self.active_sagas.get(saga_id)
        if not saga:
            return False
        
        if saga.status in [SagaStatus.PENDING, SagaStatus.IN_PROGRESS]:
            await self._start_compensation(saga)
            return True
        
        return False
    
    async def close(self):
        """Clean up resources"""
        if self._processing_task and not self._processing_task.done():
            self._processing_task.cancel()


# Example SAGA transactions
class OrderSagaBuilder:
    """Builder for order processing SAGA"""
    
    @staticmethod
    def create_order_saga(order_data: Dict[str, Any]) -> SagaTransaction:
        """Create SAGA for order processing"""
        saga_id = str(uuid.uuid4())
        
        steps = [
            SagaStep(
                step_id=f"{saga_id}_validate_inventory",
                service_name="inventory-service",
                action="validate_inventory",
                compensation_action="release_inventory",
                payload={
                    "order_id": order_data["order_id"],
                    "items": order_data["items"]
                }
            ),
            SagaStep(
                step_id=f"{saga_id}_process_payment",
                service_name="payment-service",
                action="process_payment",
                compensation_action="refund_payment",
                payload={
                    "order_id": order_data["order_id"],
                    "amount": order_data["total_amount"],
                    "payment_method": order_data["payment_method"]
                }
            ),
            SagaStep(
                step_id=f"{saga_id}_reserve_inventory",
                service_name="inventory-service",
                action="reserve_inventory",
                compensation_action="release_reservation",
                payload={
                    "order_id": order_data["order_id"],
                    "items": order_data["items"]
                }
            ),
            SagaStep(
                step_id=f"{saga_id}_create_shipment",
                service_name="shipping-service",
                action="create_shipment",
                compensation_action="cancel_shipment",
                payload={
                    "order_id": order_data["order_id"],
                    "shipping_address": order_data["shipping_address"],
                    "items": order_data["items"]
                }
            ),
            SagaStep(
                step_id=f"{saga_id}_update_order_status",
                service_name="order-service",
                action="confirm_order",
                compensation_action="cancel_order",
                payload={
                    "order_id": order_data["order_id"],
                    "status": "confirmed"
                }
            )
        ]
        
        return SagaTransaction(
            saga_id=saga_id,
            transaction_type="order_processing",
            steps=steps,
            metadata={"customer_id": order_data.get("customer_id")},
            timeout_seconds=600  # 10 minutes
        )


# Singleton orchestrator
_saga_orchestrator = None

def get_saga_orchestrator() -> SagaOrchestrator:
    """Get global SAGA orchestrator instance"""
    global _saga_orchestrator
    if _saga_orchestrator is None:
        _saga_orchestrator = SagaOrchestrator()
    return _saga_orchestrator