"""Product categorization API client for enriching product data."""
from __future__ import annotations

import re

from core.logging import get_logger

from .base_client import BaseAPIClient

logger = get_logger(__name__)


class ProductAPIClient(BaseAPIClient):
    """Client for product categorization and enrichment using ML APIs."""

    def __init__(self, api_key: str | None = None) -> None:
        """
        Initialize product API client.

        Uses a combination of local rules and external API for product categorization.
        """
        super().__init__(
            base_url="https://api.datamuse.com",  # Free word/concept API
            rate_limit_per_second=1.0,
        )

        # Predefined product categories based on retail analysis
        self.product_categories = {
            # Home & Garden
            "home_garden": {
                "keywords": ["garden", "plant", "flower", "pot", "vase", "candle", "lantern", "decoration"],
                "patterns": [r".*garden.*", r".*plant.*", r".*flower.*", r".*decoration.*"]
            },
            # Fashion & Accessories
            "fashion": {
                "keywords": ["bag", "handbag", "purse", "belt", "scarf", "hat", "jewelry", "watch"],
                "patterns": [r".*bag.*", r".*handbag.*", r".*fashion.*", r".*jewelry.*"]
            },
            # Kitchen & Dining
            "kitchen": {
                "keywords": ["kitchen", "cook", "plate", "bowl", "cup", "mug", "cutlery", "spoon"],
                "patterns": [r".*kitchen.*", r".*cook.*", r".*dining.*", r".*plate.*"]
            },
            # Toys & Games
            "toys": {
                "keywords": ["toy", "game", "puzzle", "doll", "bear", "child", "kid"],
                "patterns": [r".*toy.*", r".*game.*", r".*child.*", r".*kid.*"]
            },
            # Gifts & Souvenirs
            "gifts": {
                "keywords": ["gift", "souvenir", "memory", "keepsake", "present", "christmas", "birthday"],
                "patterns": [r".*gift.*", r".*souvenir.*", r".*christmas.*", r".*birthday.*"]
            },
            # Books & Stationery
            "books": {
                "keywords": ["book", "notebook", "pen", "pencil", "paper", "card", "stationery"],
                "patterns": [r".*book.*", r".*notebook.*", r".*stationery.*", r".*card.*"]
            },
            # Art & Craft
            "art_craft": {
                "keywords": ["art", "craft", "paint", "brush", "creative", "handmade", "vintage"],
                "patterns": [r".*art.*", r".*craft.*", r".*vintage.*", r".*handmade.*"]
            },
            # Electronics & Tech
            "electronics": {
                "keywords": ["electronic", "tech", "digital", "device", "gadget", "cable"],
                "patterns": [r".*electronic.*", r".*tech.*", r".*digital.*", r".*device.*"]
            },
        }

    async def health_check(self) -> bool:
        """Check if the product API is accessible."""
        try:
            # Test with a simple word lookup
            response = await self.get("words", params={"ml": "product"})
            return isinstance(response, list)
        except Exception as e:
            logger.error(f"Product API health check failed: {e}")
            return False

    def categorize_product_local(self, description: str, stock_code: str = "") -> dict[str, any]:
        """
        Categorize product using local rules and patterns.

        Args:
            description: Product description
            stock_code: Product stock code

        Returns:
            Product categorization information
        """
        if not description:
            return {
                "category": "uncategorized",
                "subcategory": "",
                "confidence": 0.0,
                "method": "local_rules",
                "tags": [],
            }

        # Clean and normalize description
        clean_desc = description.lower().strip()

        # Initialize scoring
        category_scores = {}
        matched_keywords = []

        # Score against each category
        for category, rules in self.product_categories.items():
            score = 0

            # Check keywords
            for keyword in rules["keywords"]:
                if keyword in clean_desc:
                    score += 1
                    matched_keywords.append(keyword)

            # Check patterns
            for pattern in rules["patterns"]:
                if re.search(pattern, clean_desc):
                    score += 2  # Patterns have higher weight

            if score > 0:
                category_scores[category] = score

        # Determine best category
        if category_scores:
            best_category = max(category_scores, key=category_scores.get)
            confidence = min(category_scores[best_category] / 5.0, 1.0)  # Normalize to 0-1
        else:
            best_category = "uncategorized"
            confidence = 0.0

        # Extract additional tags
        tags = self._extract_product_tags(clean_desc)

        # Determine subcategory based on specific keywords
        subcategory = self._determine_subcategory(best_category, clean_desc)

        return {
            "category": best_category,
            "subcategory": subcategory,
            "confidence": confidence,
            "method": "local_rules",
            "tags": tags,
            "matched_keywords": matched_keywords,
            "stock_code": stock_code,
        }

    async def get_related_words(self, word: str) -> list[str]:
        """
        Get semantically related words for product description analysis.

        Args:
            word: Input word

        Returns:
            List of related words
        """
        try:
            # Use DataMuse API to find related words
            response = await self.get("words", params={
                "ml": word,  # Words with similar meaning
                "max": 10
            })

            return [item["word"] for item in response if "word" in item]

        except Exception as e:
            logger.error(f"Failed to get related words for '{word}': {e}")
            return []

    async def enrich_product_data(
        self,
        product_data: dict
    ) -> dict:
        """
        Enrich product data with categorization and additional information.

        Args:
            product_data: Product record with description and stock_code

        Returns:
            Enriched product data with categorization
        """
        description = product_data.get("description", "")
        stock_code = product_data.get("stock_code", "")

        # Get local categorization
        categorization = self.categorize_product_local(description, stock_code)

        # Enrich with additional analysis
        enriched_data = product_data.copy()
        enriched_data.update({
            f"product_{key}": value
            for key, value in categorization.items()
        })

        # Add derived fields
        enriched_data.update({
            "product_enriched": True,
            "is_gift_item": categorization["category"] == "gifts",
            "is_seasonal": self._is_seasonal_product(description),
            "complexity_score": self._calculate_complexity_score(description),
            "brand_detected": self._detect_brand(description),
        })

        # Try to get related words for the main product category
        if categorization["confidence"] > 0.5:
            try:
                main_word = description.split()[0] if description else ""
                if main_word:
                    related_words = await self.get_related_words(main_word)
                    enriched_data["product_related_terms"] = related_words[:5]
            except Exception as e:
                logger.warning(f"Failed to get related words: {e}")
                enriched_data["product_related_terms"] = []

        return enriched_data

    def _extract_product_tags(self, description: str) -> list[str]:
        """Extract descriptive tags from product description."""
        tags = []

        # Color detection
        colors = ["red", "blue", "green", "yellow", "black", "white", "pink", "purple", "orange", "brown"]
        for color in colors:
            if color in description:
                tags.append(f"color_{color}")

        # Size detection
        sizes = ["small", "medium", "large", "mini", "giant", "tiny", "big"]
        for size in sizes:
            if size in description:
                tags.append(f"size_{size}")

        # Material detection
        materials = ["wood", "metal", "plastic", "glass", "ceramic", "fabric", "leather", "cotton"]
        for material in materials:
            if material in description:
                tags.append(f"material_{material}")

        # Style detection
        styles = ["vintage", "modern", "classic", "contemporary", "rustic", "elegant", "decorative"]
        for style in styles:
            if style in description:
                tags.append(f"style_{style}")

        return tags

    def _determine_subcategory(self, category: str, description: str) -> str:
        """Determine subcategory based on category and description."""
        subcategory_mapping = {
            "home_garden": {
                "candle": "lighting",
                "plant": "gardening",
                "decoration": "decor",
                "vintage": "antiques",
            },
            "fashion": {
                "bag": "accessories",
                "jewelry": "accessories",
                "vintage": "vintage_fashion",
            },
            "kitchen": {
                "plate": "tableware",
                "cup": "drinkware",
                "cook": "cookware",
            },
            "toys": {
                "bear": "stuffed_animals",
                "puzzle": "educational",
                "child": "kids_toys",
            },
        }

        if category in subcategory_mapping:
            for keyword, subcat in subcategory_mapping[category].items():
                if keyword in description:
                    return subcat

        return ""

    def _is_seasonal_product(self, description: str) -> bool:
        """Detect if product is seasonal."""
        seasonal_keywords = [
            "christmas", "halloween", "easter", "valentine", "summer", "winter",
            "spring", "autumn", "holiday", "seasonal", "xmas"
        ]
        return any(keyword in description.lower() for keyword in seasonal_keywords)

    def _calculate_complexity_score(self, description: str) -> float:
        """Calculate product complexity score based on description."""
        if not description:
            return 0.0

        # Simple heuristic based on description length and word count
        word_count = len(description.split())
        char_count = len(description)

        # Normalize to 0-1 scale
        complexity = min((word_count * 0.1) + (char_count * 0.001), 1.0)
        return round(complexity, 2)

    def _detect_brand(self, description: str) -> str | None:
        """Attempt to detect brand names in product description."""
        # Common brands in retail datasets (simplified)
        known_brands = [
            "nike", "adidas", "apple", "samsung", "sony", "canon", "hp", "dell",
            "coca-cola", "pepsi", "nestlé", "unilever", "procter"
        ]

        description_lower = description.lower()
        for brand in known_brands:
            if brand in description_lower:
                return brand.title()

        return None
