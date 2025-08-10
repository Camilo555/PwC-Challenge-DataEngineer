"""Product domain entity and related models."""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, List, Dict, Any
import re

from pydantic import Field, field_validator, model_validator

from ..base import DomainEntity
from ...core.constants import STOCK_CODE_PATTERN


class ProductCategory(str, Enum):
    """Product category enumeration."""

    GIFTS = "gifts"
    DECORATIVE = "decorative"
    KITCHEN = "kitchen"
    GARDEN = "garden"
    VINTAGE = "vintage"
    CHRISTMAS = "christmas"
    TOYS = "toys"
    BAGS = "bags"
    STATIONERY = "stationery"
    LIGHTING = "lighting"
    FURNITURE = "furniture"
    STORAGE = "storage"
    OTHER = "other"
    POSTAGE = "postage"
    MANUAL = "manual"
    BANK_CHARGES = "bank_charges"


class StockItem(DomainEntity):
    """Represents a stock keeping unit (SKU)."""

    stock_code: str = Field(
        ...,
        description="Unique stock code",
        min_length=1,
        max_length=20,
    )
    description: str = Field(
        ...,
        description="Product description",
        min_length=1,
        max_length=500,
    )
    unit_cost: Optional[Decimal] = Field(
        None,
        description="Unit cost",
        ge=0,
    )
    unit_price: Optional[Decimal] = Field(
        None,
        description="Standard unit price",
        ge=0,
    )
    quantity_in_stock: Optional[int] = Field(
        None,
        description="Current stock quantity",
        ge=0,
    )
    reorder_level: Optional[int] = Field(
        None,
        description="Reorder level",
        ge=0,
    )

    @field_validator("stock_code")
    @classmethod
    def validate_stock_code(cls, v: str) -> str:
        """Validate and normalize stock code."""
        return v.strip().upper()

    def validate_business_rules(self) -> bool:
        """Validate stock item against business rules."""
        self.clear_validation_errors()

        # Rule 1: Profit margin check
        if self.unit_cost and self.unit_price:
            if self.unit_price < self.unit_cost:
                self.add_validation_error(
                    f"Unit price ({self.unit_price}) is less than cost ({self.unit_cost})"
                )

        # Rule 2: Reorder level should be less than current stock
        if self.quantity_in_stock and self.reorder_level:
            if self.quantity_in_stock < self.reorder_level:
                # This is a warning, not an error
                pass  # Could trigger reorder

        return self.is_valid


class Product(DomainEntity):
    """
    Represents a product in the catalog.
    Enriched version of items from transactions.
    """

    stock_code: str = Field(
        ...,
        description="Unique product identifier",
        min_length=1,
        max_length=20,
    )
    description: str = Field(
        ...,
        description="Product description",
        min_length=1,
        max_length=500,
    )
    category: ProductCategory = Field(
        default=ProductCategory.OTHER,
        description="Product category",
    )
    subcategory: Optional[str] = Field(
        None,
        description="Product subcategory",
        max_length=100,
    )

    # Pricing information
    base_price: Decimal = Field(
        ...,
        description="Base selling price",
        ge=0,
        decimal_places=2,
    )
    min_price: Optional[Decimal] = Field(
        None,
        description="Minimum observed price",
        ge=0,
    )
    max_price: Optional[Decimal] = Field(
        None,
        description="Maximum observed price",
        ge=0,
    )
    avg_price: Optional[Decimal] = Field(
        None,
        description="Average selling price",
        ge=0,
    )

    # Product attributes
    color: Optional[str] = Field(
        None,
        description="Product color",
        max_length=50,
    )
    size: Optional[str] = Field(
        None,
        description="Product size",
        max_length=50,
    )
    material: Optional[str] = Field(
        None,
        description="Product material",
        max_length=100,
    )
    brand: Optional[str] = Field(
        None,
        description="Product brand",
        max_length=100,
    )

    # Metrics
    total_sold: Optional[int] = Field(
        None,
        description="Total quantity sold",
        ge=0,
    )
    total_returned: Optional[int] = Field(
        None,
        description="Total quantity returned",
        ge=0,
    )
    return_rate: Optional[float] = Field(
        None,
        description="Return rate percentage",
        ge=0,
        le=100,
    )

    # Flags
    is_active: bool = Field(
        default=True,
        description="Whether product is currently active",
    )
    is_seasonal: bool = Field(
        default=False,
        description="Whether product is seasonal",
    )
    is_gift: bool = Field(
        default=False,
        description="Whether product is typically a gift",
    )

    # Search optimization
    search_keywords: List[str] = Field(
        default_factory=list,
        description="Keywords for search optimization",
    )
    embedding_text: Optional[str] = Field(
        None,
        description="Text used for embedding generation",
    )

    @field_validator("stock_code")
    @classmethod
    def validate_stock_code(cls, v: str) -> str:
        """Validate and normalize stock code."""
        v = v.strip().upper()

        # Special codes
        if v in ["POST", "DOT", "M", "BANK CHARGES", "PADS", "AMAZONFEE"]:
            return v

        # Validate pattern for regular codes
        if not re.match(STOCK_CODE_PATTERN, v):
            # Log warning but don't fail
            pass

        return v

    @field_validator("description")
    @classmethod
    def clean_description(cls, v: str) -> str:
        """Clean and enhance description."""
        if v:
            v = v.strip()
            # Remove multiple spaces
            v = " ".join(v.split())
            # Capitalize properly
            v = v.title()
        return v

    @model_validator(mode="after")
    def extract_attributes(self) -> "Product":
        """Extract attributes from description."""
        if self.description:
            desc_lower = self.description.lower()

            # Extract category hints
            if any(word in desc_lower for word in ["christmas", "xmas", "noel"]):
                self.category = ProductCategory.CHRISTMAS
                self.is_seasonal = True
            elif any(word in desc_lower for word in ["vintage", "retro", "antique"]):
                self.category = ProductCategory.VINTAGE
            elif any(word in desc_lower for word in ["gift", "present"]):
                self.is_gift = True
                if self.category == ProductCategory.OTHER:
                    self.category = ProductCategory.GIFTS
            elif any(word in desc_lower for word in ["bag", "purse", "tote"]):
                self.category = ProductCategory.BAGS
            elif any(word in desc_lower for word in ["light", "lamp", "lantern"]):
                self.category = ProductCategory.LIGHTING

            # Extract color
            colors = ["red", "blue", "green", "white", "black", "pink",
                      "yellow", "purple", "orange", "brown", "grey", "silver", "gold"]
            for color in colors:
                if color in desc_lower:
                    self.color = color.title()
                    break

            # Extract size hints
            sizes = ["small", "medium", "large", "mini", "jumbo", "xl", "xxl"]
            for size in sizes:
                if size in desc_lower:
                    self.size = size.upper()
                    break

            # Generate search keywords
            self.search_keywords = [
                word for word in self.description.split()
                if len(word) > 3 and word.isalpha()
            ]

            # Generate embedding text
            attributes = []
            if self.color:
                attributes.append(self.color)
            if self.size:
                attributes.append(self.size)
            if self.category != ProductCategory.OTHER:
                attributes.append(self.category.value)

            self.embedding_text = f"{self.description} {' '.join(attributes)}".strip(
            )

        return self

    @model_validator(mode="after")
    def calculate_metrics(self) -> "Product":
        """Calculate product metrics."""
        # Calculate return rate
        if self.total_sold and self.total_returned:
            if self.total_sold > 0:
                self.return_rate = (self.total_returned /
                                    self.total_sold) * 100

        return self

    def validate_business_rules(self) -> bool:
        """Validate product against business rules."""
        self.clear_validation_errors()

        # Rule 1: Price consistency
        if self.min_price and self.max_price:
            if self.min_price > self.max_price:
                self.add_validation_error(
                    f"Min price ({self.min_price}) > Max price ({self.max_price})"
                )

            if self.base_price < self.min_price or self.base_price > self.max_price:
                self.add_validation_error(
                    f"Base price ({self.base_price}) outside of price range"
                )

        # Rule 2: Return rate threshold
        if self.return_rate and self.return_rate > 50:
            self.add_validation_error(
                f"High return rate: {self.return_rate:.2f}%"
            )

        # Rule 3: Description required
        if not self.description or len(self.description) < 3:
            self.add_validation_error("Product description too short")

        # Rule 4: Stock code format
        special_codes = ["POST", "DOT", "M",
                         "BANK CHARGES", "PADS", "AMAZONFEE"]
        if self.stock_code not in special_codes:
            if not re.match(STOCK_CODE_PATTERN, self.stock_code):
                self.add_validation_error(
                    f"Invalid stock code format: {self.stock_code}"
                )

        return self.is_valid

    def to_search_document(self) -> Dict[str, Any]:
        """Convert product to search document for indexing."""
        return {
            "id": self.stock_code,
            "stock_code": self.stock_code,
            "description": self.description,
            "category": self.category.value,
            "subcategory": self.subcategory,
            "price": float(self.base_price),
            "color": self.color,
            "size": self.size,
            "keywords": self.search_keywords,
            "embedding_text": self.embedding_text,
            "is_active": self.is_active,
            "is_seasonal": self.is_seasonal,
            "is_gift": self.is_gift,
        }
