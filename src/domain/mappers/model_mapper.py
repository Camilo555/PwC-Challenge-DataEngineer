"""
Model Mapping Layer
Provides conversion between domain entities, DTOs, and persistence models.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, TypeVar
from uuid import UUID

from pydantic import BaseModel
from sqlmodel import SQLModel

from core.logging import get_logger

logger = get_logger(__name__)

# Type variables
DomainT = TypeVar('DomainT', bound=BaseModel)
PersistenceT = TypeVar('PersistenceT', bound=SQLModel)
DTOT = TypeVar('DTOT', bound=BaseModel)


class ModelMapper:
    """Generic model mapper for converting between different model types."""

    @staticmethod
    def domain_to_persistence(domain_obj: BaseModel, persistence_class: type[SQLModel]) -> SQLModel:
        """
        Convert domain entity to persistence model.
        
        Args:
            domain_obj: Domain entity instance
            persistence_class: Target persistence model class
            
        Returns:
            Persistence model instance
        """
        try:
            # Extract dict from domain object
            domain_data = domain_obj.dict()

            # Get field mappings
            field_mappings = ModelMapper._get_field_mappings(
                domain_obj.__class__, persistence_class
            )

            # Convert field names and types
            persistence_data = {}
            for domain_field, persistence_field in field_mappings.items():
                if domain_field in domain_data:
                    value = domain_data[domain_field]
                    converted_value = ModelMapper._convert_value_for_persistence(
                        value, persistence_class, persistence_field
                    )
                    persistence_data[persistence_field] = converted_value

            # Create persistence instance
            return persistence_class(**persistence_data)

        except Exception as e:
            logger.error(f"Error converting domain to persistence: {e}")
            raise ValueError(f"Failed to convert {domain_obj.__class__.__name__} to {persistence_class.__name__}: {e}")

    @staticmethod
    def persistence_to_domain(persistence_obj: SQLModel, domain_class: type[BaseModel]) -> BaseModel:
        """
        Convert persistence model to domain entity.
        
        Args:
            persistence_obj: Persistence model instance
            domain_class: Target domain entity class
            
        Returns:
            Domain entity instance
        """
        try:
            # Extract data from SQLModel
            if hasattr(persistence_obj, '__dict__'):
                persistence_data = {
                    k: v for k, v in persistence_obj.__dict__.items()
                    if not k.startswith('_')
                }
            else:
                persistence_data = persistence_obj.dict()

            # Get field mappings (reverse)
            field_mappings = ModelMapper._get_field_mappings(
                domain_class, persistence_obj.__class__
            )
            reverse_mappings = {v: k for k, v in field_mappings.items()}

            # Convert field names and types
            domain_data = {}
            for persistence_field, domain_field in reverse_mappings.items():
                if persistence_field in persistence_data:
                    value = persistence_data[persistence_field]
                    converted_value = ModelMapper._convert_value_for_domain(
                        value, domain_class, domain_field
                    )
                    domain_data[domain_field] = converted_value

            # Apply domain validation and create instance
            return domain_class(**domain_data)

        except Exception as e:
            logger.error(f"Error converting persistence to domain: {e}")
            raise ValueError(f"Failed to convert {persistence_obj.__class__.__name__} to {domain_class.__name__}: {e}")

    @staticmethod
    def dto_to_domain(dto: BaseModel, domain_class: type[BaseModel]) -> BaseModel:
        """
        Convert DTO to domain entity.
        
        Args:
            dto: DTO instance
            domain_class: Target domain entity class
            
        Returns:
            Domain entity instance
        """
        try:
            dto_data = dto.dict(exclude_unset=True)

            # Get field mappings
            field_mappings = ModelMapper._get_field_mappings(dto.__class__, domain_class)

            # Convert data
            domain_data = {}
            for dto_field, domain_field in field_mappings.items():
                if dto_field in dto_data:
                    value = dto_data[dto_field]
                    converted_value = ModelMapper._convert_value_for_domain(
                        value, domain_class, domain_field
                    )
                    domain_data[domain_field] = converted_value

            return domain_class(**domain_data)

        except Exception as e:
            logger.error(f"Error converting DTO to domain: {e}")
            raise ValueError(f"Failed to convert {dto.__class__.__name__} to {domain_class.__name__}: {e}")

    @staticmethod
    def domain_to_dto(domain_obj: BaseModel, dto_class: type[BaseModel]) -> BaseModel:
        """
        Convert domain entity to DTO.
        
        Args:
            domain_obj: Domain entity instance
            dto_class: Target DTO class
            
        Returns:
            DTO instance
        """
        try:
            domain_data = domain_obj.dict()

            # Get field mappings
            field_mappings = ModelMapper._get_field_mappings(domain_obj.__class__, dto_class)

            # Convert data
            dto_data = {}
            for domain_field, dto_field in field_mappings.items():
                if domain_field in domain_data:
                    value = domain_data[domain_field]
                    converted_value = ModelMapper._convert_value_for_dto(
                        value, dto_class, dto_field
                    )
                    dto_data[dto_field] = converted_value

            return dto_class(**dto_data)

        except Exception as e:
            logger.error(f"Error converting domain to DTO: {e}")
            raise ValueError(f"Failed to convert {domain_obj.__class__.__name__} to {dto_class.__name__}: {e}")

    @staticmethod
    def _get_field_mappings(source_class: type, target_class: type) -> dict[str, str]:
        """Get field mappings between source and target classes."""
        source_fields = set()
        target_fields = set()

        # Get fields from source class
        if hasattr(source_class, '__fields__'):
            source_fields = set(source_class.__fields__.keys())
        elif hasattr(source_class, '__annotations__'):
            source_fields = set(source_class.__annotations__.keys())

        # Get fields from target class
        if hasattr(target_class, '__fields__'):
            target_fields = set(target_class.__fields__.keys())
        elif hasattr(target_class, '__annotations__'):
            target_fields = set(target_class.__annotations__.keys())

        # Create mappings for common fields
        mappings = {}
        for field in source_fields:
            if field in target_fields:
                mappings[field] = field

        # Add custom mappings based on naming conventions
        custom_mappings = ModelMapper._get_custom_field_mappings(source_class, target_class)
        mappings.update(custom_mappings)

        return mappings

    @staticmethod
    def _get_custom_field_mappings(source_class: type, target_class: type) -> dict[str, str]:
        """Get custom field mappings based on naming conventions."""
        mappings = {}

        # Common field name transformations
        transformations = [
            # Domain to persistence
            ('id', 'sale_id'),
            ('customer_id', 'customer_key'),
            ('product_id', 'product_key'),
            ('country_id', 'country_key'),
            ('invoice_id', 'invoice_key'),
            ('date', 'date_key'),
            # Persistence to domain
            ('sale_id', 'id'),
            ('customer_key', 'customer_id'),
            ('product_key', 'product_id'),
            ('country_key', 'country_id'),
            ('invoice_key', 'invoice_id'),
            ('date_key', 'date'),
        ]

        source_fields = ModelMapper._get_class_fields(source_class)
        target_fields = ModelMapper._get_class_fields(target_class)

        for source_field, target_field in transformations:
            if source_field in source_fields and target_field in target_fields:
                mappings[source_field] = target_field

        return mappings

    @staticmethod
    def _get_class_fields(cls: type) -> set:
        """Get field names from class."""
        if hasattr(cls, '__fields__'):
            return set(cls.__fields__.keys())
        elif hasattr(cls, '__annotations__'):
            return set(cls.__annotations__.keys())
        return set()

    @staticmethod
    def _convert_value_for_persistence(value: Any, target_class: type[SQLModel], field_name: str) -> Any:
        """Convert value for persistence model."""
        if value is None:
            return None

        # Get target field type
        target_type = ModelMapper._get_field_type(target_class, field_name)

        # Type conversions for persistence
        if target_type == float and isinstance(value, Decimal):
            return float(value)
        elif target_type == str and isinstance(value, UUID):
            return str(value)
        elif target_type == int and isinstance(value, str) and value.isdigit():
            return int(value)
        elif target_type == datetime and isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace('Z', '+00:00'))
            except:
                return value

        return value

    @staticmethod
    def _convert_value_for_domain(value: Any, target_class: type[BaseModel], field_name: str) -> Any:
        """Convert value for domain model."""
        if value is None:
            return None

        # Get target field type
        target_type = ModelMapper._get_field_type(target_class, field_name)

        # Type conversions for domain
        if target_type == Decimal and isinstance(value, (int, float)):
            return Decimal(str(value))
        elif target_type == UUID and isinstance(value, str):
            try:
                return UUID(value)
            except:
                return value
        elif target_type == str and isinstance(value, UUID):
            return str(value)
        elif target_type == datetime and isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace('Z', '+00:00'))
            except:
                return value

        return value

    @staticmethod
    def _convert_value_for_dto(value: Any, target_class: type[BaseModel], field_name: str) -> Any:
        """Convert value for DTO."""
        if value is None:
            return None

        # Get target field type
        target_type = ModelMapper._get_field_type(target_class, field_name)

        # Type conversions for DTO (usually keep as-is or convert to JSON-serializable)
        if isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, UUID):
            return str(value)
        elif isinstance(value, datetime):
            return value.isoformat()

        return value

    @staticmethod
    def _get_field_type(cls: type, field_name: str) -> type | None:
        """Get field type from class."""
        if hasattr(cls, '__fields__') and field_name in cls.__fields__:
            field_info = cls.__fields__[field_name]
            if hasattr(field_info, 'type_'):
                return field_info.type_
            elif hasattr(field_info, 'annotation'):
                return field_info.annotation
        elif hasattr(cls, '__annotations__') and field_name in cls.__annotations__:
            return cls.__annotations__[field_name]

        return None


class SalesMapper:
    """Specialized mapper for sales-related models."""

    @staticmethod
    def sale_entity_to_fact_sales(sale_entity, dimensions: dict[str, Any]) -> Any:
        """Convert sale entity to fact table record with dimension keys."""
        from data_access.models.star_schema import FactSale

        try:
            # Base mapping
            fact_data = ModelMapper.domain_to_persistence(sale_entity, FactSale).dict()

            # Add dimension keys
            fact_data.update({
                'date_key': dimensions.get('date_key'),
                'product_key': dimensions.get('product_key'),
                'customer_key': dimensions.get('customer_key'),
                'country_key': dimensions.get('country_key'),
                'invoice_key': dimensions.get('invoice_key'),
            })

            # Calculate derived measures
            if 'unit_price' in fact_data and 'quantity' in fact_data:
                fact_data['line_total'] = fact_data['unit_price'] * fact_data['quantity']

            if 'line_total' in fact_data and 'discount_amount' in fact_data:
                fact_data['net_amount'] = fact_data['line_total'] - fact_data.get('discount_amount', 0)

            return FactSale(**fact_data)

        except Exception as e:
            logger.error(f"Error mapping sale entity to fact sales: {e}")
            raise


class CustomerMapper:
    """Specialized mapper for customer-related models."""

    @staticmethod
    def customer_with_analytics(customer_entity, analytics_data: dict[str, Any]) -> Any:
        """Convert customer entity with analytics data to dimension record."""
        from data_access.models.star_schema import DimCustomer

        try:
            # Base mapping
            customer_data = ModelMapper.domain_to_persistence(customer_entity, DimCustomer).dict()

            # Add analytics data
            customer_data.update({
                'customer_lifetime_value_tier': analytics_data.get('clv_tier'),
                'churn_risk_score': analytics_data.get('churn_risk'),
                'preferred_channel': analytics_data.get('preferred_channel'),
                'days_since_first_purchase': analytics_data.get('days_since_first_purchase'),
                'recency_score': analytics_data.get('recency_score'),
                'frequency_score': analytics_data.get('frequency_score'),
                'monetary_score': analytics_data.get('monetary_score'),
                'rfm_segment': analytics_data.get('rfm_segment'),
            })

            return DimCustomer(**customer_data)

        except Exception as e:
            logger.error(f"Error mapping customer with analytics: {e}")
            raise


class ProductMapper:
    """Specialized mapper for product-related models."""

    @staticmethod
    def product_with_hierarchy(product_entity, hierarchy_data: dict[str, Any]) -> Any:
        """Convert product entity with hierarchy data to dimension record."""
        from data_access.models.star_schema import DimProduct

        try:
            # Base mapping
            product_data = ModelMapper.domain_to_persistence(product_entity, DimProduct).dict()

            # Add hierarchy data
            product_data.update({
                'category': hierarchy_data.get('category'),
                'subcategory': hierarchy_data.get('subcategory'),
                'brand': hierarchy_data.get('brand'),
                'price_tier': hierarchy_data.get('price_tier'),
                'margin_category': hierarchy_data.get('margin_category'),
            })

            return DimProduct(**product_data)

        except Exception as e:
            logger.error(f"Error mapping product with hierarchy: {e}")
            raise


# Batch mapping utilities

class BatchMapper:
    """Utilities for batch mapping operations."""

    @staticmethod
    def map_batch(items: list[Any], mapper_func: callable, **kwargs) -> list[Any]:
        """Map a batch of items using provided mapper function."""
        results = []
        errors = []

        for i, item in enumerate(items):
            try:
                mapped_item = mapper_func(item, **kwargs)
                results.append(mapped_item)
            except Exception as e:
                error_detail = {
                    'index': i,
                    'item': item,
                    'error': str(e)
                }
                errors.append(error_detail)
                logger.error(f"Error mapping item at index {i}: {e}")

        return results, errors

    @staticmethod
    def map_with_validation(items: list[Any], mapper_func: callable,
                          validator_func: callable | None = None, **kwargs) -> dict[str, Any]:
        """Map items with optional validation."""
        mapped_items = []
        validation_errors = []
        mapping_errors = []

        for i, item in enumerate(items):
            try:
                # Map item
                mapped_item = mapper_func(item, **kwargs)

                # Validate if validator provided
                if validator_func:
                    validation_result = validator_func(mapped_item)
                    if not validation_result.get('is_valid', True):
                        validation_errors.append({
                            'index': i,
                            'item': item,
                            'errors': validation_result.get('errors', [])
                        })
                        continue

                mapped_items.append(mapped_item)

            except Exception as e:
                mapping_errors.append({
                    'index': i,
                    'item': item,
                    'error': str(e)
                })
                logger.error(f"Error mapping item at index {i}: {e}")

        return {
            'mapped_items': mapped_items,
            'mapping_errors': mapping_errors,
            'validation_errors': validation_errors,
            'success_count': len(mapped_items),
            'error_count': len(mapping_errors) + len(validation_errors)
        }
