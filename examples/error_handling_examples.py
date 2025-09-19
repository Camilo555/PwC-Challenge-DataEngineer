#!/usr/bin/env python3
"""
Standardized Error Handling Examples
====================================

Comprehensive examples demonstrating how to use the standardized error handling
system across different modules and scenarios.
"""
import asyncio
import time
from typing import Dict, Any, List, Optional

# Import standardized error handling components
from src.core.exceptions import (
    # Decorators
    handle_exceptions,
    validate_input,
    retry_on_error,
    measure_performance,
    circuit_breaker,
    api_endpoint,
    data_processing,
    external_service,

    # Context managers
    handle_validation_errors,
    database_operation,
    external_service_call,
    data_processing_batch,
    business_operation,
    safe_call,

    # Patterns
    get_validation_pattern,
    get_database_pattern,
    get_external_service_pattern,
    get_business_rule_pattern,
    get_data_processing_pattern,

    # Exceptions
    ValidationError,
    DatabaseError,
    ExternalServiceError,
    DataProcessingError,
    ErrorCode
)


class ErrorHandlingExamples:
    """Examples of standardized error handling across different scenarios."""

    def __init__(self):
        """Initialize examples with pattern instances."""
        self.validation_pattern = get_validation_pattern()
        self.database_pattern = get_database_pattern()
        self.external_service_pattern = get_external_service_pattern()
        self.business_rule_pattern = get_business_rule_pattern()
        self.data_processing_pattern = get_data_processing_pattern()

    # =============================================================================
    # 1. Decorator Examples
    # =============================================================================

    @handle_exceptions(
        exceptions=ValueError,
        error_code=ErrorCode.VALIDATION_ERROR,
        user_message="Invalid input provided"
    )
    def example_basic_error_handling(self, value: int) -> int:
        """Example of basic error handling with decorator."""
        if value < 0:
            raise ValueError("Value must be positive")
        return value * 2

    @validate_input(
        validator=lambda *args, **kwargs: kwargs.get('email', '').count('@') == 1,
        error_message="Invalid email format",
        field_name="email"
    )
    def example_input_validation(self, email: str) -> str:
        """Example of input validation decorator."""
        return f"Processing email: {email}"

    @retry_on_error(
        max_retries=3,
        delay=1.0,
        backoff=2.0,
        exceptions=ConnectionError
    )
    def example_retry_pattern(self, fail_times: int = 2) -> str:
        """Example of retry pattern with exponential backoff."""
        if not hasattr(self, '_call_count'):
            self._call_count = 0

        self._call_count += 1

        if self._call_count <= fail_times:
            raise ConnectionError(f"Connection failed (attempt {self._call_count})")

        return f"Success after {self._call_count} attempts"

    @measure_performance(
        operation_name="data_transformation",
        slow_threshold_seconds=2.0
    )
    def example_performance_monitoring(self, processing_time: float) -> Dict[str, Any]:
        """Example of performance monitoring."""
        time.sleep(processing_time)  # Simulate processing
        return {"processed": True, "duration": processing_time}

    @circuit_breaker(
        failure_threshold=3,
        recovery_timeout=10.0,
        expected_exception=ConnectionError
    )
    def example_circuit_breaker(self, should_fail: bool = False) -> str:
        """Example of circuit breaker pattern."""
        if should_fail:
            raise ConnectionError("Service unavailable")
        return "Service response"

    @api_endpoint(require_auth=False, validate_input=True, handle_errors=True)
    def example_api_endpoint(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Example of API endpoint with composite decorators."""
        # Simulate API processing
        return {
            "status": "success",
            "data": request_data,
            "timestamp": time.time()
        }

    @data_processing(retry_on_failure=True, max_retries=2)
    def example_data_processing(self, data: List[Dict]) -> List[Dict]:
        """Example of data processing with error handling."""
        processed = []
        for item in data:
            # Simulate processing that might fail
            if not item.get('valid', True):
                raise ValueError(f"Invalid data item: {item}")
            processed.append({**item, "processed": True})
        return processed

    @external_service(use_circuit_breaker=True, retry_attempts=2)
    async def example_external_service_call(self, service_url: str) -> Dict[str, Any]:
        """Example of external service call with comprehensive error handling."""
        # Simulate external service call
        await asyncio.sleep(0.1)  # Simulate network delay
        return {"service_url": service_url, "response": "success"}

    # =============================================================================
    # 2. Context Manager Examples
    # =============================================================================

    def example_validation_context(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Example using validation context manager."""
        with handle_validation_errors("user_registration") as errors:
            # Collect validation errors
            if not user_data.get('email'):
                errors.append("Email is required")
            if not user_data.get('password') or len(user_data['password']) < 8:
                errors.append("Password must be at least 8 characters")
            if user_data.get('age', 0) < 18:
                errors.append("User must be at least 18 years old")

        return {"status": "valid", "data": user_data}

    def example_database_context(self, table_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Example using database operation context manager."""
        with database_operation("insert", table_name) as db_context:
            # Simulate database operations
            db_context['query_count'] = 1
            db_context['affected_rows'] = 1

            # Simulate potential database error
            if table_name == "invalid_table":
                raise Exception("Table does not exist")

            return {"inserted_id": 123, "table": table_name, "data": data}

    def example_external_service_context(self, service_name: str) -> Dict[str, Any]:
        """Example using external service call context manager."""
        with external_service_call(service_name, "get_data", timeout_seconds=5.0) as service_context:
            # Simulate service call
            time.sleep(0.1)
            service_context['response_status'] = 200
            service_context['response_size'] = 1024

            return {"service": service_name, "data": "response_data"}

    def example_data_processing_context(self, records: List[Dict]) -> Dict[str, Any]:
        """Example using data processing batch context manager."""
        with data_processing_batch("user_import", total_records=len(records)) as batch_context:
            for i, record in enumerate(records):
                try:
                    # Simulate processing
                    if not record.get('valid', True):
                        batch_context['failed_records'] += 1
                        batch_context['errors'].append(f"Invalid record at index {i}")
                    else:
                        batch_context['processed_records'] += 1
                except Exception as e:
                    batch_context['failed_records'] += 1
                    batch_context['errors'].append(str(e))

            return {
                "total": len(records),
                "processed": batch_context['processed_records'],
                "failed": batch_context['failed_records']
            }

    def example_business_operation_context(self, customer_data: Dict[str, Any]) -> Dict[str, Any]:
        """Example using business operation context manager."""
        with business_operation("customer_credit_check", customer_data) as business_context:
            # Validate business rules
            business_context['rules_validated'].append("credit_score_check")
            business_context['rules_validated'].append("income_verification")

            # Simulate business rule violation
            if customer_data.get('credit_score', 0) < 600:
                business_context['rule_violations'].append("Credit score too low")
                raise ValueError("Credit score below minimum threshold")

            return {"approved": True, "customer_id": customer_data.get('id')}

    # =============================================================================
    # 3. Pattern Examples
    # =============================================================================

    def example_validation_patterns(self) -> Dict[str, Any]:
        """Example using validation patterns."""
        user_data = {
            "email": "user@example.com",
            "age": 25,
            "name": "John Doe"
        }

        # Validate required fields
        required_errors = self.validation_pattern.validate_required_fields(
            user_data, ["email", "name", "phone"]  # phone is missing
        )

        # Validate field types
        type_errors = self.validation_pattern.validate_field_types(
            user_data, {"email": str, "age": int}
        )

        # Validate ranges
        range_errors = self.validation_pattern.validate_field_ranges(
            user_data, {"age": {"min": 18, "max": 120}}
        )

        all_errors = required_errors + type_errors + range_errors

        if all_errors:
            validation_error = self.validation_pattern.create_validation_error(all_errors, "user_data")
            print(f"Validation errors: {validation_error.field_errors}")

        return {
            "valid": len(all_errors) == 0,
            "errors": all_errors,
            "data": user_data
        }

    def example_database_patterns(self) -> Dict[str, Any]:
        """Example using database patterns."""
        results = {}

        # Simulate connection error
        try:
            conn_error = ConnectionError("Connection timeout")
            db_error = self.database_pattern.handle_connection_error(
                conn_error, "postgresql://user:pass@localhost/db"
            )
        except DatabaseError as e:
            results['connection_error'] = e.to_dict()

        # Simulate query error
        try:
            query_error = Exception("Table 'users' doesn't exist")
            db_error = self.database_pattern.handle_query_error(
                query_error, "SELECT * FROM users", {"limit": 10}
            )
        except DatabaseError as e:
            results['query_error'] = e.to_dict()

        # Simulate constraint violation
        try:
            constraint_error = Exception("UNIQUE constraint failed")
            db_error = self.database_pattern.handle_constraint_violation(
                constraint_error, "users", "unique_email"
            )
        except DatabaseError as e:
            results['constraint_error'] = e.to_dict()

        return results

    def example_external_service_patterns(self) -> Dict[str, Any]:
        """Example using external service patterns."""
        results = {}

        # Simulate timeout error
        try:
            timeout_error = TimeoutError("Request timed out")
            service_error = self.external_service_pattern.handle_timeout_error(
                timeout_error, "payment_service", 30.0
            )
        except ExternalServiceError as e:
            results['timeout_error'] = e.to_dict()

        # Simulate authentication error
        try:
            auth_error = Exception("Invalid API key")
            service_error = self.external_service_pattern.handle_authentication_error(
                auth_error, "payment_service", "api_key"
            )
        except ExternalServiceError as e:
            results['auth_error'] = e.to_dict()

        # Simulate rate limit error
        try:
            rate_error = Exception("Rate limit exceeded")
            service_error = self.external_service_pattern.handle_rate_limit_error(
                rate_error, "payment_service", retry_after=60
            )
        except ExternalServiceError as e:
            results['rate_limit_error'] = e.to_dict()

        return results

    def example_business_rule_patterns(self) -> Dict[str, Any]:
        """Example using business rule patterns."""
        results = {}

        # Test customer spending limit
        try:
            self.business_rule_pattern.validate_customer_limits(
                "customer_123", 1500.0, 1000.0  # Exceeds limit
            )
        except Exception as e:
            results['spending_limit'] = str(e)

        # Test inventory availability
        try:
            self.business_rule_pattern.validate_inventory_availability(
                "product_456", 5, 3  # Not enough inventory
            )
        except Exception as e:
            results['inventory_check'] = str(e)

        return results

    def example_data_processing_patterns(self) -> Dict[str, Any]:
        """Example using data processing patterns."""
        results = {}

        # Simulate data quality errors
        try:
            quality_errors = ["Missing required field", "Invalid email format", "Date out of range"]
            processing_error = self.data_processing_pattern.handle_data_quality_error(
                quality_errors, record_count=100, failed_count=15
            )
        except DataProcessingError as e:
            results['quality_error'] = e.to_dict()

        # Simulate transformation error
        try:
            transform_error = ValueError("Cannot convert string to integer")
            processing_error = self.data_processing_pattern.handle_transformation_error(
                transform_error, "data_normalization", {"field": "age", "value": "invalid"}
            )
        except DataProcessingError as e:
            results['transformation_error'] = e.to_dict()

        return results

    # =============================================================================
    # 4. Utility Examples
    # =============================================================================

    def example_safe_call_patterns(self) -> Dict[str, Any]:
        """Example using safe call utilities."""
        def potentially_failing_function(x: int) -> int:
            if x < 0:
                raise ValueError("Negative values not allowed")
            return x * 2

        # Safe synchronous call
        result1 = safe_call(potentially_failing_function, 5, default=0)
        result2 = safe_call(potentially_failing_function, -1, default=0, log_errors=True)

        async def potentially_failing_async_function(x: int) -> int:
            await asyncio.sleep(0.1)
            if x < 0:
                raise ValueError("Negative values not allowed")
            return x * 3

        return {
            "safe_call_success": result1,  # Should be 10
            "safe_call_failure": result2,  # Should be 0 (default)
        }

    def example_comprehensive_error_handling(self) -> Dict[str, Any]:
        """Comprehensive example combining multiple patterns."""
        results = {
            "operations": [],
            "errors": [],
            "summary": {}
        }

        # Operation 1: Validate user input
        try:
            with handle_validation_errors("comprehensive_example") as errors:
                user_data = {"email": "invalid-email", "age": 15}

                if '@' not in user_data.get('email', ''):
                    errors.append("Invalid email format")
                if user_data.get('age', 0) < 18:
                    errors.append("Age must be at least 18")

            results['operations'].append("validation_passed")
        except ValidationError as e:
            results['errors'].append(f"Validation failed: {e.message}")

        # Operation 2: Database operation with retry
        @retry_on_error(max_retries=2, delay=0.1)
        def database_operation_with_retry():
            with database_operation("select", "users") as db_context:
                # Simulate intermittent database issue
                import random
                if random.random() < 0.3:  # 30% chance of failure
                    raise ConnectionError("Database connection lost")

                db_context['query_count'] = 1
                return {"users": [{"id": 1, "name": "John"}]}

        try:
            data = database_operation_with_retry()
            results['operations'].append("database_success")
            results['user_data'] = data
        except Exception as e:
            results['errors'].append(f"Database operation failed: {str(e)}")

        # Operation 3: External service call with circuit breaker
        @circuit_breaker(failure_threshold=2, recovery_timeout=1.0)
        def external_api_call():
            with external_service_call("user_service", "get_profile") as service_context:
                # Simulate service response
                service_context['response_status'] = 200
                return {"profile": {"id": 1, "preferences": {}}}

        try:
            profile_data = external_api_call()
            results['operations'].append("external_service_success")
            results['profile_data'] = profile_data
        except Exception as e:
            results['errors'].append(f"External service failed: {str(e)}")

        # Summary
        results['summary'] = {
            'total_operations': len(results['operations']),
            'successful_operations': len(results['operations']),
            'failed_operations': len(results['errors']),
            'success_rate': len(results['operations']) / (len(results['operations']) + len(results['errors'])) * 100
        }

        return results


async def run_examples():
    """Run all error handling examples."""
    examples = ErrorHandlingExamples()

    print("=" * 80)
    print("Standardized Error Handling Examples")
    print("=" * 80)

    # 1. Decorator Examples
    print("\n1. Decorator Examples")
    print("-" * 50)

    # Basic error handling
    try:
        result = examples.example_basic_error_handling(10)
        print(f"✓ Basic error handling success: {result}")
    except Exception as e:
        print(f"✗ Basic error handling failed: {e}")

    # Input validation
    try:
        result = examples.example_input_validation("user@example.com")
        print(f"✓ Input validation success: {result}")
    except Exception as e:
        print(f"✗ Input validation failed: {e}")

    # Retry pattern
    try:
        result = examples.example_retry_pattern(2)
        print(f"✓ Retry pattern success: {result}")
    except Exception as e:
        print(f"✗ Retry pattern failed: {e}")

    # Performance monitoring
    result = examples.example_performance_monitoring(0.5)
    print(f"✓ Performance monitoring: {result}")

    # API endpoint
    result = examples.example_api_endpoint({"test": "data"})
    print(f"✓ API endpoint: {result['status']}")

    # Data processing
    try:
        data = [{"id": 1, "valid": True}, {"id": 2, "valid": True}]
        result = examples.example_data_processing(data)
        print(f"✓ Data processing: {len(result)} items processed")
    except Exception as e:
        print(f"✗ Data processing failed: {e}")

    # External service
    try:
        result = await examples.example_external_service_call("https://api.example.com")
        print(f"✓ External service call: {result['response']}")
    except Exception as e:
        print(f"✗ External service call failed: {e}")

    # 2. Context Manager Examples
    print("\n2. Context Manager Examples")
    print("-" * 50)

    # Validation context
    try:
        result = examples.example_validation_context({"email": "test@example.com", "password": "password123", "age": 25})
        print(f"✓ Validation context: {result['status']}")
    except ValidationError as e:
        print(f"✗ Validation context failed: {e.field_errors}")

    # Database context
    try:
        result = examples.example_database_context("users", {"name": "John"})
        print(f"✓ Database context: inserted_id={result['inserted_id']}")
    except Exception as e:
        print(f"✗ Database context failed: {e}")

    # External service context
    try:
        result = examples.example_external_service_context("payment_service")
        print(f"✓ External service context: {result['service']}")
    except Exception as e:
        print(f"✗ External service context failed: {e}")

    # Data processing context
    records = [{"valid": True}, {"valid": False}, {"valid": True}]
    try:
        result = examples.example_data_processing_context(records)
        print(f"✓ Data processing context: {result['processed']}/{result['total']} processed")
    except Exception as e:
        print(f"✗ Data processing context failed: {e}")

    # Business operation context
    try:
        result = examples.example_business_operation_context({"id": "123", "credit_score": 750})
        print(f"✓ Business operation context: approved={result['approved']}")
    except Exception as e:
        print(f"✗ Business operation context failed: {e}")

    # 3. Pattern Examples
    print("\n3. Pattern Examples")
    print("-" * 50)

    # Validation patterns
    validation_result = examples.example_validation_patterns()
    print(f"✓ Validation patterns: valid={validation_result['valid']}, errors={len(validation_result['errors'])}")

    # Database patterns
    db_result = examples.example_database_patterns()
    print(f"✓ Database patterns: {len(db_result)} error types handled")

    # External service patterns
    service_result = examples.example_external_service_patterns()
    print(f"✓ External service patterns: {len(service_result)} error types handled")

    # Business rule patterns
    business_result = examples.example_business_rule_patterns()
    print(f"✓ Business rule patterns: {len(business_result)} rules validated")

    # Data processing patterns
    processing_result = examples.example_data_processing_patterns()
    print(f"✓ Data processing patterns: {len(processing_result)} error types handled")

    # 4. Utility Examples
    print("\n4. Utility Examples")
    print("-" * 50)

    # Safe call patterns
    safe_result = examples.example_safe_call_patterns()
    print(f"✓ Safe call patterns: success={safe_result['safe_call_success']}, failure_default={safe_result['safe_call_failure']}")

    # Comprehensive example
    comprehensive_result = examples.example_comprehensive_error_handling()
    summary = comprehensive_result['summary']
    print(f"✓ Comprehensive example: {summary['successful_operations']}/{summary['total_operations']} operations successful ({summary['success_rate']:.1f}%)")

    print("\n" + "=" * 80)
    print("All error handling examples completed!")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(run_examples())