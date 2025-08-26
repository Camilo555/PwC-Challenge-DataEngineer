"""Currency exchange rate API client for data enrichment."""
from __future__ import annotations

from datetime import datetime

from core.config import settings
from core.logging import get_logger

from .base_client import BaseAPIClient

logger = get_logger(__name__)


class CurrencyAPIClient(BaseAPIClient):
    """Client for fetching currency exchange rates."""

    def __init__(self, api_key: str | None = None) -> None:
        """
        Initialize currency API client.

        Uses exchangerate-api.com which provides free tier with 1500 requests/month.
        """
        super().__init__(
            base_url="https://v6.exchangerate-api.com/v6",
            api_key=api_key or settings.currency_api_key,
            rate_limit_per_second=0.5,  # Conservative rate limiting
        )

    async def health_check(self) -> bool:
        """Check if the currency API is accessible."""
        try:
            # Use a simple supported-codes endpoint for health check
            response = await self.get("supported-codes")
            return response.get("result") == "success"
        except Exception as e:
            logger.error(f"Currency API health check failed: {e}")
            return False

    async def get_exchange_rates(
        self,
        base_currency: str = "GBP",
        target_currencies: list[str] | None = None
    ) -> dict[str, float]:
        """
        Get current exchange rates from base currency to targets.

        Args:
            base_currency: Base currency code (default: GBP for UK retail data)
            target_currencies: List of target currency codes

        Returns:
            Dictionary mapping currency codes to exchange rates
        """
        if target_currencies is None:
            target_currencies = ["USD", "EUR", "CAD", "AUD", "JPY"]

        try:
            if self.api_key:
                endpoint = f"{self.api_key}/latest/{base_currency}"
            else:
                # Fallback to free endpoint (limited functionality)
                endpoint = f"latest/{base_currency}"

            response = await self.get(endpoint)

            if response.get("result") == "success":
                conversion_rates = response.get("conversion_rates", {})

                # Filter to requested currencies
                filtered_rates = {
                    currency: rate
                    for currency, rate in conversion_rates.items()
                    if currency in target_currencies
                }

                logger.info(f"Retrieved exchange rates for {len(filtered_rates)} currencies")
                return filtered_rates
            else:
                logger.error(f"Currency API error: {response.get('error-type', 'Unknown error')}")
                return {}

        except Exception as e:
            logger.error(f"Failed to fetch exchange rates: {e}")
            return {}

    async def get_historical_rates(
        self,
        base_currency: str,
        target_currency: str,
        date: datetime
    ) -> float | None:
        """
        Get historical exchange rate for a specific date.

        Args:
            base_currency: Base currency code
            target_currency: Target currency code
            date: Date for historical rate

        Returns:
            Exchange rate or None if not available
        """
        try:
            if not self.api_key:
                logger.warning("Historical rates require API key")
                return None

            date_str = date.strftime("%Y-%m-%d")
            endpoint = f"{self.api_key}/history/{base_currency}/{date_str}"

            response = await self.get(endpoint)

            if response.get("result") == "success":
                conversion_rates = response.get("conversion_rates", {})
                return conversion_rates.get(target_currency)
            else:
                logger.error(f"Historical rate API error: {response.get('error-type')}")
                return None

        except Exception as e:
            logger.error(f"Failed to fetch historical rate: {e}")
            return None

    async def enrich_transaction_with_rates(
        self,
        transaction_data: dict,
        base_currency: str = "GBP"
    ) -> dict:
        """
        Enrich transaction data with current exchange rates.

        Args:
            transaction_data: Transaction record
            base_currency: Base currency for conversion

        Returns:
            Enriched transaction data with exchange rates
        """
        try:
            # Get exchange rates
            rates = await self.get_exchange_rates(base_currency)

            # Add exchange rate information
            enriched_data = transaction_data.copy()
            enriched_data.update({
                "base_currency": base_currency,
                "exchange_rates": rates,
                "enrichment_timestamp": datetime.utcnow().isoformat(),
            })

            # Calculate amounts in different currencies if unit_price exists
            if "unit_price" in transaction_data and rates:
                unit_price = float(transaction_data.get("unit_price", 0))
                quantity = float(transaction_data.get("quantity", 1))

                for currency_code, rate in rates.items():
                    enriched_data[f"unit_price_{currency_code.lower()}"] = round(unit_price * rate, 2)
                    enriched_data[f"amount_{currency_code.lower()}"] = round(unit_price * quantity * rate, 2)

            return enriched_data

        except Exception as e:
            logger.error(f"Failed to enrich transaction with currency rates: {e}")
            # Return original data with error flag
            enriched_data = transaction_data.copy()
            enriched_data.update({
                "enrichment_error": f"Currency enrichment failed: {str(e)}",
                "enrichment_timestamp": datetime.utcnow().isoformat(),
            })
            return enriched_data
