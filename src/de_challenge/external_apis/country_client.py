"""Country information API client for geographic data enrichment."""

from typing import Dict, List, Optional

from de_challenge.core.logging import get_logger
from .base_client import BaseAPIClient

logger = get_logger(__name__)


class CountryAPIClient(BaseAPIClient):
    """Client for fetching country information and geographic data."""

    def __init__(self) -> None:
        """
        Initialize country API client.
        
        Uses restcountries.com which is free and doesn't require API key.
        """
        super().__init__(
            base_url="https://restcountries.com/v3.1",
            rate_limit_per_second=2.0,  # Be respectful to free service
        )

    async def health_check(self) -> bool:
        """Check if the country API is accessible."""
        try:
            # Try to get a simple country info
            response = await self.get("name/united kingdom")
            return isinstance(response, list) and len(response) > 0
        except Exception as e:
            logger.error(f"Country API health check failed: {e}")
            return False

    async def get_country_info(self, country_name: str) -> Optional[Dict]:
        """
        Get detailed information about a country.

        Args:
            country_name: Name of the country

        Returns:
            Country information dictionary or None if not found
        """
        try:
            # Clean and normalize country name
            normalized_name = country_name.strip().lower()
            
            # Handle common variations in the retail dataset
            name_mappings = {
                "uk": "united kingdom",
                "usa": "united states",
                "uae": "united arab emirates",
                "rsa": "south africa",
                "eire": "ireland",
            }
            
            normalized_name = name_mappings.get(normalized_name, normalized_name)
            
            endpoint = f"name/{normalized_name}"
            response = await self.get(endpoint)
            
            if isinstance(response, list) and len(response) > 0:
                country_data = response[0]  # Take the first match
                
                # Extract relevant information
                return {
                    "official_name": country_data.get("name", {}).get("official", ""),
                    "common_name": country_data.get("name", {}).get("common", ""),
                    "country_code_2": country_data.get("cca2", ""),
                    "country_code_3": country_data.get("cca3", ""),
                    "region": country_data.get("region", ""),
                    "subregion": country_data.get("subregion", ""),
                    "continent": country_data.get("continents", [""])[0] if country_data.get("continents") else "",
                    "capital": country_data.get("capital", [""])[0] if country_data.get("capital") else "",
                    "population": country_data.get("population", 0),
                    "area": country_data.get("area", 0),
                    "timezone": country_data.get("timezones", [""])[0] if country_data.get("timezones") else "",
                    "currency_codes": [
                        code for code in country_data.get("currencies", {}).keys()
                    ],
                    "languages": list(country_data.get("languages", {}).values()),
                    "flag_emoji": country_data.get("flag", ""),
                    "landlocked": country_data.get("landlocked", False),
                    "lat_lng": country_data.get("latlng", [0, 0]),
                }
            else:
                logger.warning(f"Country not found: {country_name}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to fetch country info for '{country_name}': {e}")
            return None

    async def get_countries_by_region(self, region: str) -> List[Dict]:
        """
        Get all countries in a specific region.

        Args:
            region: Region name (e.g., 'Europe', 'Asia')

        Returns:
            List of country information dictionaries
        """
        try:
            endpoint = f"region/{region}"
            response = await self.get(endpoint)
            
            countries = []
            for country_data in response:
                country_info = {
                    "name": country_data.get("name", {}).get("common", ""),
                    "code": country_data.get("cca2", ""),
                    "population": country_data.get("population", 0),
                    "area": country_data.get("area", 0),
                }
                countries.append(country_info)
            
            return countries
            
        except Exception as e:
            logger.error(f"Failed to fetch countries for region '{region}': {e}")
            return []

    async def enrich_transaction_with_country_info(
        self,
        transaction_data: Dict
    ) -> Dict:
        """
        Enrich transaction data with detailed country information.

        Args:
            transaction_data: Transaction record with country field

        Returns:
            Enriched transaction data with country details
        """
        country_name = transaction_data.get("country")
        if not country_name:
            logger.warning("No country field found in transaction data")
            return transaction_data

        # Get country information
        country_info = await self.get_country_info(country_name)
        
        enriched_data = transaction_data.copy()
        
        if country_info:
            # Add country information with prefixes to avoid conflicts
            enriched_data.update({
                f"country_{key}": value
                for key, value in country_info.items()
            })
            
            # Add derived fields
            enriched_data.update({
                "country_enriched": True,
                "is_european": country_info.get("region") == "Europe",
                "is_landlocked": country_info.get("landlocked", False),
                "country_size_category": self._categorize_country_size(
                    country_info.get("population", 0)
                ),
            })
        else:
            enriched_data.update({
                "country_enriched": False,
                "country_enrichment_error": f"Country info not found: {country_name}",
            })

        return enriched_data

    def _categorize_country_size(self, population: int) -> str:
        """Categorize country by population size."""
        if population > 100_000_000:
            return "Large"
        elif population > 10_000_000:
            return "Medium"
        elif population > 1_000_000:
            return "Small"
        else:
            return "Micro"