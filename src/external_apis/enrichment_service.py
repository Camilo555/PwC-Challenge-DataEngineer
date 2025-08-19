"""Main enrichment service orchestrating multiple external APIs."""

import asyncio

from core.logging import get_logger

from .country_client import CountryAPIClient
from .currency_client import CurrencyAPIClient
from .product_client import ProductAPIClient

logger = get_logger(__name__)


class DataEnrichmentService:
    """Service for enriching transaction data with external APIs."""

    def __init__(self) -> None:
        """Initialize enrichment service with API clients."""
        self.currency_client = CurrencyAPIClient()
        self.country_client = CountryAPIClient()
        self.product_client = ProductAPIClient()

    async def health_check_all(self) -> dict[str, bool]:
        """Check health of all external APIs."""
        logger.info("Performing health check on all external APIs...")

        # Run health checks concurrently
        currency_health, country_health, product_health = await asyncio.gather(
            self.currency_client.health_check(),
            self.country_client.health_check(),
            self.product_client.health_check(),
            return_exceptions=True
        )

        # Handle exceptions
        health_status = {
            "currency_api": currency_health if isinstance(currency_health, bool) else False,
            "country_api": country_health if isinstance(country_health, bool) else False,
            "product_api": product_health if isinstance(product_health, bool) else False,
        }

        logger.info(f"API health check results: {health_status}")
        return health_status

    async def enrich_single_transaction(
        self,
        transaction_data: dict,
        include_currency: bool = True,
        include_country: bool = True,
        include_product: bool = True,
    ) -> dict:
        """
        Enrich a single transaction with all available external data.

        Args:
            transaction_data: Raw transaction data
            include_currency: Whether to include currency enrichment
            include_country: Whether to include country enrichment
            include_product: Whether to include product enrichment

        Returns:
            Enriched transaction data
        """
        enriched_data = transaction_data.copy()
        enrichment_tasks = []

        # Prepare enrichment tasks
        if include_currency:
            enrichment_tasks.append(
                asyncio.create_task(
                    self.currency_client.enrich_transaction_with_rates(transaction_data)
                )
            )
        else:
            enrichment_tasks.append(
                asyncio.create_task(self._return_as_is(transaction_data))
            )

        if include_country:
            enrichment_tasks.append(
                asyncio.create_task(
                    self.country_client.enrich_transaction_with_country_info(transaction_data)
                )
            )
        else:
            enrichment_tasks.append(
                asyncio.create_task(self._return_as_is(transaction_data))
            )

        if include_product:
            enrichment_tasks.append(
                asyncio.create_task(
                    self.product_client.enrich_product_data(transaction_data)
                )
            )
        else:
            enrichment_tasks.append(
                asyncio.create_task(self._return_as_is(transaction_data))
            )

        try:
            # Run enrichments concurrently
            currency_enriched, country_enriched, product_enriched = await asyncio.gather(
                *enrichment_tasks,
                return_exceptions=True
            )

            # Merge enrichment results
            enriched_data = self._merge_enrichment_results(
                enriched_data,
                currency_enriched if not isinstance(currency_enriched, Exception) else {},
                country_enriched if not isinstance(country_enriched, Exception) else {},
                product_enriched if not isinstance(product_enriched, Exception) else {},
            )

            # Add enrichment metadata
            enriched_data.update({
                "enrichment_completed": True,
                "enrichment_errors": self._collect_enrichment_errors(
                    currency_enriched, country_enriched, product_enriched
                ),
                "enrichment_services_used": {
                    "currency": include_currency and not isinstance(currency_enriched, Exception),
                    "country": include_country and not isinstance(country_enriched, Exception),
                    "product": include_product and not isinstance(product_enriched, Exception),
                }
            })

        except Exception as e:
            logger.error(f"Error during transaction enrichment: {e}")
            enriched_data.update({
                "enrichment_completed": False,
                "enrichment_error": str(e),
            })

        return enriched_data

    async def enrich_batch_transactions(
        self,
        transactions: list[dict],
        batch_size: int = 10,
        include_currency: bool = True,
        include_country: bool = True,
        include_product: bool = True,
    ) -> list[dict]:
        """
        Enrich multiple transactions in batches to respect rate limits.

        Args:
            transactions: List of transaction data
            batch_size: Number of transactions to process concurrently
            include_currency: Whether to include currency enrichment
            include_country: Whether to include country enrichment
            include_product: Whether to include product enrichment

        Returns:
            List of enriched transaction data
        """
        logger.info(f"Starting batch enrichment of {len(transactions)} transactions")

        enriched_transactions = []

        # Process in batches
        for i in range(0, len(transactions), batch_size):
            batch = transactions[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}: transactions {i+1}-{min(i+batch_size, len(transactions))}")

            # Enrich batch concurrently
            batch_tasks = [
                self.enrich_single_transaction(
                    transaction,
                    include_currency=include_currency,
                    include_country=include_country,
                    include_product=include_product,
                )
                for transaction in batch
            ]

            try:
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)

                # Handle results and exceptions
                for j, result in enumerate(batch_results):
                    if isinstance(result, Exception):
                        logger.error(f"Failed to enrich transaction {i+j+1}: {result}")
                        # Keep original transaction with error marker
                        error_transaction = batch[j].copy()
                        error_transaction.update({
                            "enrichment_completed": False,
                            "enrichment_error": str(result),
                        })
                        enriched_transactions.append(error_transaction)
                    else:
                        enriched_transactions.append(result)

            except Exception as e:
                logger.error(f"Batch processing error: {e}")
                # Add original transactions with error markers
                for transaction in batch:
                    error_transaction = transaction.copy()
                    error_transaction.update({
                        "enrichment_completed": False,
                        "enrichment_error": f"Batch processing failed: {e}",
                    })
                    enriched_transactions.append(error_transaction)

            # Small delay between batches to be respectful to APIs
            if i + batch_size < len(transactions):
                await asyncio.sleep(1.0)

        logger.info(f"Completed enrichment of {len(enriched_transactions)} transactions")
        return enriched_transactions

    async def get_enrichment_statistics(self, enriched_transactions: list[dict]) -> dict:
        """
        Generate statistics about the enrichment process.

        Args:
            enriched_transactions: List of enriched transactions

        Returns:
            Enrichment statistics
        """
        total_transactions = len(enriched_transactions)
        if total_transactions == 0:
            return {"total_transactions": 0}

        successful_enrichments = sum(
            1 for t in enriched_transactions
            if t.get("enrichment_completed", False)
        )

        currency_enriched = sum(
            1 for t in enriched_transactions
            if t.get("enrichment_services_used", {}).get("currency", False)
        )

        country_enriched = sum(
            1 for t in enriched_transactions
            if t.get("enrichment_services_used", {}).get("country", False)
        )

        product_enriched = sum(
            1 for t in enriched_transactions
            if t.get("enrichment_services_used", {}).get("product", False)
        )

        return {
            "total_transactions": total_transactions,
            "successful_enrichments": successful_enrichments,
            "success_rate": successful_enrichments / total_transactions,
            "currency_enriched": currency_enriched,
            "country_enriched": country_enriched,
            "product_enriched": product_enriched,
            "enrichment_coverage": {
                "currency": currency_enriched / total_transactions,
                "country": country_enriched / total_transactions,
                "product": product_enriched / total_transactions,
            }
        }

    async def close_all_clients(self) -> None:
        """Close all API client sessions."""
        await asyncio.gather(
            self.currency_client.close(),
            self.country_client.close(),
            self.product_client.close(),
            return_exceptions=True
        )

    async def _return_as_is(self, data: dict) -> dict:
        """Helper to return data unchanged for disabled enrichments."""
        return data

    def _merge_enrichment_results(
        self,
        base_data: dict,
        currency_data: dict,
        country_data: dict,
        product_data: dict,
    ) -> dict:
        """Merge results from different enrichment services."""
        merged = base_data.copy()

        # Merge currency data
        for key, value in currency_data.items():
            if key not in merged or key.startswith(("amount_", "unit_price_", "exchange_", "base_currency")):
                merged[key] = value

        # Merge country data
        for key, value in country_data.items():
            if key not in merged or key.startswith("country_"):
                merged[key] = value

        # Merge product data
        for key, value in product_data.items():
            if key not in merged or key.startswith("product_"):
                merged[key] = value

        return merged

    def _collect_enrichment_errors(self, *results) -> list[str]:
        """Collect errors from enrichment results."""
        errors = []
        service_names = ["currency", "country", "product"]

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                errors.append(f"{service_names[i]}: {str(result)}")
            elif isinstance(result, dict) and result.get("enrichment_error"):
                errors.append(f"{service_names[i]}: {result['enrichment_error']}")

        return errors


# Global enrichment service instance
_enrichment_service: DataEnrichmentService | None = None


def get_enrichment_service() -> DataEnrichmentService:
    """Get global enrichment service instance."""
    global _enrichment_service
    if _enrichment_service is None:
        _enrichment_service = DataEnrichmentService()
    return _enrichment_service
