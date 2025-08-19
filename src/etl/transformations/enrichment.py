"""
Real-time Data Enrichment Module
Provides external API integrations and data augmentation capabilities
"""
from __future__ import annotations

import pandas as pd
import numpy as np
import requests
import asyncio
import aiohttp
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib

from core.config import settings
from core.logging import get_logger

logger = get_logger(__name__)


class ExternalDataEnricher:
    """Enriches data with external APIs and services."""
    
    def __init__(self, batch_size: int = 10, rate_limit_delay: float = 0.1):
        self.batch_size = batch_size
        self.rate_limit_delay = rate_limit_delay
        self.cache = {}  # Simple in-memory cache
        
    def enrich_with_currency_rates(self, df: pd.DataFrame, 
                                  base_currency: str = 'GBP',
                                  target_currency: str = 'USD') -> pd.DataFrame:
        """Enrich data with currency exchange rates."""
        logger.info(f"Enriching with currency rates: {base_currency} to {target_currency}")
        
        if not settings.currency_api_key or settings.currency_api_key == 'your_exchangerate_api_key_here':
            logger.warning("Currency API key not configured, using mock data")
            return self._add_mock_currency_data(df, base_currency, target_currency)
        
        # Get unique dates for currency rate lookup
        if 'invoice_timestamp' not in df.columns:
            logger.warning("No timestamp column found for currency enrichment")
            return df
        
        df['date'] = pd.to_datetime(df['invoice_timestamp']).dt.date
        unique_dates = df['date'].unique()
        
        # Fetch rates for each unique date
        currency_rates = {}
        for date in unique_dates:
            rate = self._get_currency_rate(date, base_currency, target_currency)
            currency_rates[date] = rate
        
        # Add rates to dataframe
        df[f'exchange_rate_{base_currency}_to_{target_currency}'] = df['date'].map(currency_rates)
        
        # Calculate converted prices
        if 'unit_price' in df.columns:
            df[f'unit_price_{target_currency}'] = (
                df['unit_price'] * df[f'exchange_rate_{base_currency}_to_{target_currency}']
            )
        
        return df
    
    def _get_currency_rate(self, date: datetime.date, base: str, target: str) -> float:
        """Get currency exchange rate for specific date."""
        cache_key = f"{date}_{base}_{target}"
        
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            # Using exchangerate-api.com (free tier available)
            url = f"https://api.exchangerate-api.com/v4/historical/{date.strftime('%Y-%m-%d')}"
            
            time.sleep(self.rate_limit_delay)  # Rate limiting
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                rates = data.get('rates', {})
                
                if base == 'USD':
                    rate = rates.get(target, 1.0)
                elif target == 'USD':
                    rate = 1.0 / rates.get(base, 1.0)
                else:
                    # Convert through USD
                    usd_to_target = rates.get(target, 1.0)
                    usd_to_base = rates.get(base, 1.0)
                    rate = usd_to_target / usd_to_base
                
                self.cache[cache_key] = rate
                return rate
            else:
                logger.warning(f"Currency API returned status {response.status_code}")
                return 1.0
                
        except Exception as e:
            logger.warning(f"Failed to fetch currency rate: {e}")
            return 1.0
    
    def _add_mock_currency_data(self, df: pd.DataFrame, base: str, target: str) -> pd.DataFrame:
        """Add mock currency data for testing."""
        # Generate realistic exchange rates with some variation
        np.random.seed(42)
        base_rate = 1.25 if base == 'GBP' and target == 'USD' else 1.0
        
        df[f'exchange_rate_{base}_to_{target}'] = np.random.normal(
            base_rate, 0.05, len(df)
        ).clip(min=0.1)
        
        if 'unit_price' in df.columns:
            df[f'unit_price_{target}'] = (
                df['unit_price'] * df[f'exchange_rate_{base}_to_{target}']
            )
        
        return df
    
    def enrich_with_country_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich with country metadata (timezone, region, etc.)."""
        logger.info("Enriching with country metadata")
        
        # Country mapping with metadata
        country_metadata = {
            'United Kingdom': {
                'continent': 'Europe',
                'timezone': 'GMT',
                'currency': 'GBP',
                'region': 'Western Europe',
                'economic_classification': 'Developed'
            },
            'Germany': {
                'continent': 'Europe',
                'timezone': 'CET',
                'currency': 'EUR',
                'region': 'Western Europe',
                'economic_classification': 'Developed'
            },
            'France': {
                'continent': 'Europe',
                'timezone': 'CET',
                'currency': 'EUR',
                'region': 'Western Europe',
                'economic_classification': 'Developed'
            },
            'Australia': {
                'continent': 'Oceania',
                'timezone': 'AEST',
                'currency': 'AUD',
                'region': 'Oceania',
                'economic_classification': 'Developed'
            },
            'United States': {
                'continent': 'North America',
                'timezone': 'EST',
                'currency': 'USD',
                'region': 'North America',
                'economic_classification': 'Developed'
            }
        }
        
        if 'country' not in df.columns:
            logger.warning("No country column found for enrichment")
            return df
        
        # Create mapping dataframe
        country_df = pd.DataFrame.from_dict(country_metadata, orient='index').reset_index()
        country_df.columns = ['country'] + [f'country_{col}' for col in country_df.columns[1:]]
        
        # Merge with main dataframe
        df = df.merge(country_df, on='country', how='left')
        
        # Fill missing values for unknown countries
        fill_values = {
            'country_continent': 'Unknown',
            'country_timezone': 'UTC',
            'country_currency': 'USD',
            'country_region': 'Unknown',
            'country_economic_classification': 'Unknown'
        }
        
        for col, value in fill_values.items():
            if col in df.columns:
                df[col] = df[col].fillna(value)
        
        logger.info(f"Enriched with country metadata for {df['country'].nunique()} countries")
        return df
    
    def enrich_with_product_categories(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich with AI-generated product categories based on description."""
        logger.info("Enriching with product categories")
        
        if 'description' not in df.columns:
            logger.warning("No description column found for categorization")
            return df
        
        # Simple rule-based categorization (could be enhanced with ML)
        category_rules = {
            'Electronics': ['laptop', 'phone', 'computer', 'electronic', 'digital', 'tech'],
            'Clothing': ['shirt', 'dress', 'pants', 'jacket', 'clothing', 'apparel', 'fashion'],
            'Home & Garden': ['home', 'garden', 'furniture', 'decor', 'kitchen', 'bedroom'],
            'Books & Media': ['book', 'magazine', 'dvd', 'cd', 'media', 'publication'],
            'Sports & Outdoors': ['sport', 'outdoor', 'fitness', 'exercise', 'game', 'ball'],
            'Health & Beauty': ['beauty', 'health', 'cosmetic', 'skincare', 'wellness'],
            'Toys & Games': ['toy', 'game', 'puzzle', 'doll', 'action figure', 'board game'],
            'Food & Beverage': ['food', 'drink', 'beverage', 'snack', 'coffee', 'tea'],
            'Automotive': ['car', 'auto', 'vehicle', 'automotive', 'motor', 'tire'],
            'Office Supplies': ['office', 'pen', 'paper', 'notebook', 'stapler', 'supplies']
        }
        
        def categorize_product(description):
            if pd.isna(description):
                return 'Other'
                
            description_lower = str(description).lower()
            
            for category, keywords in category_rules.items():
                if any(keyword in description_lower for keyword in keywords):
                    return category
            
            return 'Other'
        
        df['product_category'] = df['description'].apply(categorize_product)
        
        # Add category hierarchy
        category_hierarchy = {
            'Electronics': 'Technology',
            'Clothing': 'Fashion & Apparel',
            'Home & Garden': 'Home & Living',
            'Books & Media': 'Entertainment',
            'Sports & Outdoors': 'Recreation',
            'Health & Beauty': 'Personal Care',
            'Toys & Games': 'Recreation',
            'Food & Beverage': 'Consumables',
            'Automotive': 'Transportation',
            'Office Supplies': 'Business',
            'Other': 'Miscellaneous'
        }
        
        df['product_category_group'] = df['product_category'].map(category_hierarchy)
        
        logger.info(f"Categorized products into {df['product_category'].nunique()} categories")
        return df
    
    def enrich_with_weather_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich with historical weather data (mock implementation)."""
        logger.info("Adding weather context data")
        
        if 'country' not in df.columns or 'invoice_timestamp' not in df.columns:
            logger.warning("Required columns for weather data not found")
            return df
        
        # Mock weather data (in real implementation, would use weather API)
        np.random.seed(42)
        
        # Generate realistic weather patterns by country and season
        def generate_weather(country, timestamp):
            if pd.isna(timestamp):
                return {'temperature': 20, 'condition': 'Clear', 'humidity': 50}
            
            month = pd.to_datetime(timestamp).month
            
            # Base temperatures by country (Celsius)
            base_temps = {
                'United Kingdom': 12,
                'Germany': 10,
                'France': 14,
                'Australia': 22,
                'United States': 16
            }
            
            base_temp = base_temps.get(country, 15)
            
            # Seasonal variation
            seasonal_variation = 10 * np.sin(2 * np.pi * (month - 3) / 12)
            temperature = base_temp + seasonal_variation + np.random.normal(0, 5)
            
            # Weather conditions based on temperature and randomness
            conditions = ['Clear', 'Cloudy', 'Rainy', 'Partly Cloudy']
            weights = [0.4, 0.3, 0.2, 0.1] if temperature > 15 else [0.2, 0.3, 0.4, 0.1]
            condition = np.random.choice(conditions, p=weights)
            
            humidity = max(30, min(90, 60 + np.random.normal(0, 15)))
            
            return {
                'temperature': round(temperature, 1),
                'condition': condition,
                'humidity': round(humidity)
            }
        
        weather_data = df.apply(
            lambda row: generate_weather(row['country'], row['invoice_timestamp']), 
            axis=1
        )
        
        df['weather_temperature'] = [w['temperature'] for w in weather_data]
        df['weather_condition'] = [w['condition'] for w in weather_data]
        df['weather_humidity'] = [w['humidity'] for w in weather_data]
        
        logger.info("Added weather context data")
        return df
    
    def enrich_with_economic_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich with economic indicators (mock implementation)."""
        logger.info("Adding economic indicators")
        
        if 'country' not in df.columns or 'invoice_timestamp' not in df.columns:
            logger.warning("Required columns for economic data not found")
            return df
        
        # Mock economic indicators (in real implementation, would use economic APIs)
        economic_data = {
            'United Kingdom': {'gdp_growth': 2.1, 'inflation': 2.3, 'unemployment': 4.2},
            'Germany': {'gdp_growth': 1.8, 'inflation': 1.9, 'unemployment': 3.5},
            'France': {'gdp_growth': 1.5, 'inflation': 2.1, 'unemployment': 8.1},
            'Australia': {'gdp_growth': 2.8, 'inflation': 2.4, 'unemployment': 5.2},
            'United States': {'gdp_growth': 2.3, 'inflation': 2.6, 'unemployment': 3.7}
        }
        
        # Create economic indicators dataframe
        econ_df = pd.DataFrame.from_dict(economic_data, orient='index').reset_index()
        econ_df.columns = ['country'] + [f'economic_{col}' for col in econ_df.columns[1:]]
        
        # Merge with main dataframe
        df = df.merge(econ_df, on='country', how='left')
        
        # Fill missing values
        fill_values = {
            'economic_gdp_growth': 2.0,
            'economic_inflation': 2.5,
            'economic_unemployment': 5.0
        }
        
        for col, value in fill_values.items():
            if col in df.columns:
                df[col] = df[col].fillna(value)
        
        logger.info("Added economic indicators")
        return df
    
    def apply_all_enrichments(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all available enrichments."""
        logger.info("Starting comprehensive data enrichment pipeline...")
        
        original_cols = len(df.columns)
        
        # Apply enrichments in sequence
        df = self.enrich_with_country_data(df)
        df = self.enrich_with_product_categories(df)
        df = self.enrich_with_currency_rates(df)
        df = self.enrich_with_weather_data(df)
        df = self.enrich_with_economic_indicators(df)
        
        new_cols = len(df.columns)
        logger.info(f"Data enrichment complete: added {new_cols - original_cols} new columns")
        
        return df


class SyntheticDataGenerator:
    """Generate synthetic data for testing and training."""
    
    def __init__(self, seed: int = 42):
        np.random.seed(seed)
        
    def generate_customer_personas(self, n_customers: int = 1000) -> pd.DataFrame:
        """Generate synthetic customer personas."""
        logger.info(f"Generating {n_customers} synthetic customer personas")
        
        # Customer demographics
        age_groups = ['18-25', '26-35', '36-45', '46-55', '56-65', '65+']
        income_brackets = ['Low', 'Medium-Low', 'Medium', 'Medium-High', 'High']
        education_levels = ['High School', 'Bachelor', 'Master', 'PhD']
        occupations = ['Student', 'Professional', 'Manager', 'Executive', 'Retired', 'Other']
        
        personas = []
        for i in range(n_customers):
            persona = {
                'customer_id': f'CUST_{i:06d}',
                'age_group': np.random.choice(age_groups),
                'income_bracket': np.random.choice(income_brackets),
                'education_level': np.random.choice(education_levels),
                'occupation': np.random.choice(occupations),
                'family_size': np.random.randint(1, 6),
                'urban_rural': np.random.choice(['Urban', 'Suburban', 'Rural'], p=[0.5, 0.3, 0.2]),
                'tech_savviness': np.random.choice(['Low', 'Medium', 'High'], p=[0.3, 0.5, 0.2]),
                'brand_loyalty': np.random.uniform(0, 1),
                'price_sensitivity': np.random.uniform(0, 1),
                'online_preference': np.random.uniform(0, 1)
            }
            personas.append(persona)
        
        return pd.DataFrame(personas)
    
    def generate_product_catalog(self, n_products: int = 500) -> pd.DataFrame:
        """Generate synthetic product catalog."""
        logger.info(f"Generating {n_products} synthetic products")
        
        categories = ['Electronics', 'Clothing', 'Home & Garden', 'Books & Media', 
                     'Sports & Outdoors', 'Health & Beauty', 'Toys & Games']
        
        products = []
        for i in range(n_products):
            category = np.random.choice(categories)
            
            # Category-specific price ranges
            price_ranges = {
                'Electronics': (50, 2000),
                'Clothing': (20, 200),
                'Home & Garden': (15, 500),
                'Books & Media': (5, 50),
                'Sports & Outdoors': (25, 300),
                'Health & Beauty': (10, 100),
                'Toys & Games': (10, 150)
            }
            
            min_price, max_price = price_ranges[category]
            
            product = {
                'stock_code': f'PROD_{i:06d}',
                'category': category,
                'base_price': round(np.random.uniform(min_price, max_price), 2),
                'cost': round(np.random.uniform(min_price * 0.4, min_price * 0.7), 2),
                'brand_tier': np.random.choice(['Budget', 'Mid-range', 'Premium'], p=[0.4, 0.5, 0.1]),
                'seasonal_factor': round(np.random.uniform(0.8, 1.2), 2),
                'popularity_score': round(np.random.uniform(0, 1), 2),
                'review_rating': round(np.random.uniform(2.5, 5.0), 1),
                'launch_date': pd.Timestamp('2020-01-01') + pd.Timedelta(days=np.random.randint(0, 1000))
            }
            products.append(product)
        
        return pd.DataFrame(products)


def enrich_data_external(df: pd.DataFrame, 
                        include_currency: bool = True,
                        include_weather: bool = True,
                        include_economic: bool = True) -> pd.DataFrame:
    """Main function to enrich data with external sources."""
    enricher = ExternalDataEnricher()
    
    if include_currency:
        df = enricher.enrich_with_currency_rates(df)
    
    df = enricher.enrich_with_country_data(df)
    df = enricher.enrich_with_product_categories(df)
    
    if include_weather:
        df = enricher.enrich_with_weather_data(df)
    
    if include_economic:
        df = enricher.enrich_with_economic_indicators(df)
    
    return df


def main():
    """Test the enrichment module."""
    print("Data Enrichment Module loaded successfully")
    print("Available enrichments:")
    print("- Currency exchange rates")
    print("- Country metadata (timezone, region, etc.)")
    print("- Product categorization")
    print("- Weather context data")
    print("- Economic indicators")
    print("- Synthetic data generation")


if __name__ == "__main__":
    main()