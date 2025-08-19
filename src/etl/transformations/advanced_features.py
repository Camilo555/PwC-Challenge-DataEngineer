"""
Advanced Feature Engineering and Data Transformations
Provides sophisticated data enrichment, customer segmentation, and business metrics
"""
from __future__ import annotations

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.cluster import KMeans
import warnings

from core.logging import get_logger

logger = get_logger(__name__)
warnings.filterwarnings('ignore')


class AdvancedFeatureEngineer:
    """Advanced feature engineering for retail analytics."""
    
    def __init__(self):
        self.scaler = StandardScaler()
        self.label_encoders = {}
        
    def create_temporal_features(self, df: pd.DataFrame, date_col: str = 'invoice_timestamp') -> pd.DataFrame:
        """Create comprehensive temporal features."""
        logger.info("Creating temporal features...")
        
        if date_col not in df.columns:
            logger.warning(f"Date column {date_col} not found, skipping temporal features")
            return df
        
        # Ensure datetime format
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        
        # Basic temporal features
        df['year'] = df[date_col].dt.year
        df['month'] = df[date_col].dt.month
        df['day'] = df[date_col].dt.day
        df['hour'] = df[date_col].dt.hour
        df['day_of_week'] = df[date_col].dt.dayofweek
        df['week_of_year'] = df[date_col].dt.isocalendar().week
        df['quarter'] = df[date_col].dt.quarter
        
        # Advanced temporal features
        df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
        df['is_month_start'] = df[date_col].dt.is_month_start.astype(int)
        df['is_month_end'] = df[date_col].dt.is_month_end.astype(int)
        df['is_quarter_start'] = df[date_col].dt.is_quarter_start.astype(int)
        df['is_quarter_end'] = df[date_col].dt.is_quarter_end.astype(int)
        
        # Business time features
        df['is_business_hours'] = ((df['hour'] >= 9) & (df['hour'] <= 17)).astype(int)
        df['is_peak_hours'] = ((df['hour'] >= 10) & (df['hour'] <= 14)).astype(int)
        
        # Season categorization
        df['season'] = df['month'].map({
            12: 'Winter', 1: 'Winter', 2: 'Winter',
            3: 'Spring', 4: 'Spring', 5: 'Spring',
            6: 'Summer', 7: 'Summer', 8: 'Summer',
            9: 'Autumn', 10: 'Autumn', 11: 'Autumn'
        })
        
        # Holiday indicators (basic - can be extended)
        df['is_december'] = (df['month'] == 12).astype(int)  # Christmas season
        df['is_january'] = (df['month'] == 1).astype(int)    # New Year
        
        logger.info(f"Created {len([col for col in df.columns if col not in [date_col]]) - len(df.columns) + 1} temporal features")
        return df
    
    def create_rfm_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create RFM (Recency, Frequency, Monetary) analysis features."""
        logger.info("Creating RFM features...")
        
        required_cols = ['customer_id', 'invoice_timestamp', 'quantity', 'unit_price']
        if not all(col in df.columns for col in required_cols):
            logger.warning("Required columns for RFM analysis not found, skipping")
            return df
        
        # Calculate total amount
        df['total_amount'] = df['quantity'] * df['unit_price']
        
        # Reference date (most recent date in dataset)
        reference_date = df['invoice_timestamp'].max()
        
        # RFM calculations
        rfm = df.groupby('customer_id').agg({
            'invoice_timestamp': lambda x: (reference_date - x.max()).days,  # Recency
            'invoice_no': 'nunique',  # Frequency
            'total_amount': 'sum'     # Monetary
        }).reset_index()
        
        rfm.columns = ['customer_id', 'recency_days', 'frequency_orders', 'monetary_total']
        
        # Create RFM scores (1-5 scale)
        rfm['recency_score'] = pd.qcut(rfm['recency_days'], 5, labels=[5,4,3,2,1], duplicates='drop')
        rfm['frequency_score'] = pd.qcut(rfm['frequency_orders'].rank(method='first'), 5, labels=[1,2,3,4,5], duplicates='drop')
        rfm['monetary_score'] = pd.qcut(rfm['monetary_total'], 5, labels=[1,2,3,4,5], duplicates='drop')
        
        # Combined RFM score
        rfm['rfm_score'] = (rfm['recency_score'].astype(str) + 
                           rfm['frequency_score'].astype(str) + 
                           rfm['monetary_score'].astype(str))
        
        # RFM segments
        def categorize_rfm(row):
            score = int(row['recency_score']) + int(row['frequency_score']) + int(row['monetary_score'])
            if score >= 12:
                return 'Champions'
            elif score >= 9:
                return 'Loyal Customers'
            elif score >= 6:
                return 'Potential Loyalists'
            elif score >= 3:
                return 'At Risk'
            else:
                return 'Lost Customers'
        
        rfm['rfm_segment'] = rfm.apply(categorize_rfm, axis=1)
        
        # Merge back to original dataframe
        df = df.merge(rfm[['customer_id', 'recency_days', 'frequency_orders', 
                          'monetary_total', 'rfm_score', 'rfm_segment']], 
                     on='customer_id', how='left')
        
        logger.info(f"Created RFM features for {rfm['customer_id'].nunique()} customers")
        return df
    
    def create_product_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create advanced product-level features."""
        logger.info("Creating product features...")
        
        if 'stock_code' not in df.columns:
            logger.warning("Stock code column not found, skipping product features")
            return df
        
        # Product popularity metrics
        product_stats = df.groupby('stock_code').agg({
            'quantity': ['sum', 'mean', 'std', 'count'],
            'unit_price': ['mean', 'std', 'min', 'max'],
            'customer_id': 'nunique',
            'invoice_no': 'nunique'
        }).reset_index()
        
        # Flatten column names
        product_stats.columns = ['stock_code'] + [
            f"product_{col[0]}_{col[1]}" if col[1] else f"product_{col[0]}" 
            for col in product_stats.columns[1:]
        ]
        
        # Additional product metrics
        product_stats['product_revenue'] = (product_stats['product_quantity_sum'] * 
                                          product_stats['product_unit_price_mean'])
        product_stats['product_price_volatility'] = (product_stats['product_unit_price_std'] / 
                                                    product_stats['product_unit_price_mean']).fillna(0)
        
        # Product categories (based on price ranges)
        product_stats['product_price_category'] = pd.cut(
            product_stats['product_unit_price_mean'],
            bins=[0, 1, 5, 20, 100, float('inf')],
            labels=['Budget', 'Low', 'Medium', 'High', 'Premium']
        )
        
        # Merge back to original dataframe
        df = df.merge(product_stats, on='stock_code', how='left')
        
        logger.info(f"Created product features for {product_stats['stock_code'].nunique()} products")
        return df
    
    def create_customer_behavior_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create customer behavior and preference features."""
        logger.info("Creating customer behavior features...")
        
        required_cols = ['customer_id', 'invoice_timestamp', 'quantity', 'unit_price']
        if not all(col in df.columns for col in required_cols):
            logger.warning("Required columns for behavior analysis not found, skipping")
            return df
        
        # Customer aggregations
        customer_stats = df.groupby('customer_id').agg({
            'quantity': ['sum', 'mean', 'std'],
            'unit_price': ['mean', 'std'],
            'invoice_no': 'nunique',
            'stock_code': 'nunique',
            'country': lambda x: x.mode().iloc[0] if not x.mode().empty else 'Unknown',
            'invoice_timestamp': ['min', 'max', 'count']
        }).reset_index()
        
        # Flatten column names
        customer_stats.columns = ['customer_id'] + [
            f"customer_{col[0]}_{col[1]}" if col[1] else f"customer_{col[0]}" 
            for col in customer_stats.columns[1:]
        ]
        
        # Calculate customer tenure and activity
        customer_stats['customer_tenure_days'] = (
            customer_stats['customer_invoice_timestamp_max'] - 
            customer_stats['customer_invoice_timestamp_min']
        ).dt.days
        
        customer_stats['customer_avg_days_between_orders'] = (
            customer_stats['customer_tenure_days'] / 
            customer_stats['customer_invoice_no_nunique']
        ).fillna(0)
        
        # Customer value metrics
        customer_stats['customer_total_spend'] = (
            customer_stats['customer_quantity_sum'] * 
            customer_stats['customer_unit_price_mean']
        )
        
        customer_stats['customer_avg_order_value'] = (
            customer_stats['customer_total_spend'] / 
            customer_stats['customer_invoice_no_nunique']
        )
        
        # Customer diversity metrics
        customer_stats['customer_product_diversity'] = customer_stats['customer_stock_code_nunique']
        customer_stats['customer_loyalty_score'] = (
            customer_stats['customer_invoice_no_nunique'] / 
            customer_stats['customer_tenure_days'].replace(0, 1)
        ) * 100
        
        # Merge back to original dataframe
        df = df.merge(customer_stats[[
            'customer_id', 'customer_total_spend', 'customer_avg_order_value',
            'customer_product_diversity', 'customer_loyalty_score',
            'customer_tenure_days', 'customer_avg_days_between_orders'
        ]], on='customer_id', how='left')
        
        logger.info(f"Created behavior features for {customer_stats['customer_id'].nunique()} customers")
        return df
    
    def create_market_basket_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create market basket analysis features."""
        logger.info("Creating market basket features...")
        
        if not all(col in df.columns for col in ['invoice_no', 'stock_code']):
            logger.warning("Required columns for market basket analysis not found, skipping")
            return df
        
        # Calculate basket size and diversity
        basket_stats = df.groupby('invoice_no').agg({
            'stock_code': ['count', 'nunique'],
            'quantity': 'sum',
            'unit_price': 'mean'
        }).reset_index()
        
        basket_stats.columns = ['invoice_no', 'basket_size', 'basket_diversity', 
                               'basket_total_quantity', 'basket_avg_price']
        
        # Calculate basket value
        basket_stats['basket_total_value'] = (
            basket_stats['basket_total_quantity'] * 
            basket_stats['basket_avg_price']
        )
        
        # Basket categories
        basket_stats['basket_size_category'] = pd.cut(
            basket_stats['basket_size'],
            bins=[0, 1, 3, 10, float('inf')],
            labels=['Single', 'Small', 'Medium', 'Large']
        )
        
        # Merge back to original dataframe
        df = df.merge(basket_stats, on='invoice_no', how='left')
        
        logger.info(f"Created market basket features for {basket_stats['invoice_no'].nunique()} baskets")
        return df
    
    def create_anomaly_detection_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create features for anomaly detection."""
        logger.info("Creating anomaly detection features...")
        
        # Quantity anomalies
        if 'quantity' in df.columns:
            q75, q25 = np.percentile(df['quantity'], [75, 25])
            iqr = q75 - q25
            lower_bound = q25 - (1.5 * iqr)
            upper_bound = q75 + (1.5 * iqr)
            
            df['quantity_outlier'] = (
                (df['quantity'] < lower_bound) | 
                (df['quantity'] > upper_bound)
            ).astype(int)
            
            df['quantity_zscore'] = np.abs(
                (df['quantity'] - df['quantity'].mean()) / df['quantity'].std()
            )
        
        # Price anomalies
        if 'unit_price' in df.columns:
            q75, q25 = np.percentile(df['unit_price'], [75, 25])
            iqr = q75 - q25
            lower_bound = q25 - (1.5 * iqr)
            upper_bound = q75 + (1.5 * iqr)
            
            df['price_outlier'] = (
                (df['unit_price'] < lower_bound) | 
                (df['unit_price'] > upper_bound)
            ).astype(int)
            
            df['price_zscore'] = np.abs(
                (df['unit_price'] - df['unit_price'].mean()) / df['unit_price'].std()
            )
        
        # Transaction value anomalies
        if 'quantity' in df.columns and 'unit_price' in df.columns:
            df['transaction_value'] = df['quantity'] * df['unit_price']
            
            q75, q25 = np.percentile(df['transaction_value'], [75, 25])
            iqr = q75 - q25
            lower_bound = q25 - (1.5 * iqr)
            upper_bound = q75 + (1.5 * iqr)
            
            df['transaction_value_outlier'] = (
                (df['transaction_value'] < lower_bound) | 
                (df['transaction_value'] > upper_bound)
            ).astype(int)
        
        logger.info("Created anomaly detection features")
        return df
    
    def create_seasonality_features(self, df: pd.DataFrame, date_col: str = 'invoice_timestamp') -> pd.DataFrame:
        """Create advanced seasonality and trend features."""
        logger.info("Creating seasonality features...")
        
        if date_col not in df.columns:
            logger.warning(f"Date column {date_col} not found, skipping seasonality features")
            return df
        
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        
        # Cyclical encoding for time features
        df['month_sin'] = np.sin(2 * np.pi * df[date_col].dt.month / 12)
        df['month_cos'] = np.cos(2 * np.pi * df[date_col].dt.month / 12)
        df['day_sin'] = np.sin(2 * np.pi * df[date_col].dt.day / 31)
        df['day_cos'] = np.cos(2 * np.pi * df[date_col].dt.day / 31)
        df['hour_sin'] = np.sin(2 * np.pi * df[date_col].dt.hour / 24)
        df['hour_cos'] = np.cos(2 * np.pi * df[date_col].dt.hour / 24)
        
        # Days since epoch (for trend analysis)
        epoch = pd.Timestamp('2010-01-01')
        df['days_since_epoch'] = (df[date_col] - epoch).dt.days
        
        # Moving averages (if enough data)
        if len(df) > 30:
            df = df.sort_values(date_col)
            
            # 7-day moving averages for quantity and price
            if 'quantity' in df.columns:
                df['quantity_ma7'] = df['quantity'].rolling(window=7, min_periods=1).mean()
                df['quantity_trend'] = df['quantity'] - df['quantity_ma7']
            
            if 'unit_price' in df.columns:
                df['price_ma7'] = df['unit_price'].rolling(window=7, min_periods=1).mean()
                df['price_trend'] = df['unit_price'] - df['price_ma7']
        
        logger.info("Created seasonality and trend features")
        return df
    
    def perform_customer_segmentation(self, df: pd.DataFrame, n_clusters: int = 5) -> pd.DataFrame:
        """Perform K-means customer segmentation based on RFM and behavior."""
        logger.info(f"Performing customer segmentation with {n_clusters} clusters...")
        
        # Required features for segmentation
        segmentation_features = [
            'customer_total_spend', 'customer_avg_order_value',
            'customer_product_diversity', 'customer_loyalty_score'
        ]
        
        if not all(col in df.columns for col in segmentation_features):
            logger.warning("Required features for segmentation not found, skipping")
            return df
        
        # Prepare customer-level data
        customer_data = df.groupby('customer_id')[segmentation_features].first().fillna(0)
        
        # Scale features
        scaled_features = self.scaler.fit_transform(customer_data)
        
        # Perform K-means clustering
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        customer_data['customer_segment'] = kmeans.fit_predict(scaled_features)
        
        # Create segment labels
        segment_labels = {
            0: 'High Value',
            1: 'Loyal Regular',
            2: 'Occasional Buyer',
            3: 'New Customer',
            4: 'At Risk'
        }
        
        customer_data['customer_segment_label'] = customer_data['customer_segment'].map(
            lambda x: segment_labels.get(x, f'Segment_{x}')
        )
        
        # Merge back to original dataframe
        df = df.merge(
            customer_data[['customer_segment', 'customer_segment_label']].reset_index(),
            on='customer_id', how='left'
        )
        
        logger.info(f"Created {n_clusters} customer segments")
        return df
    
    def apply_all_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all advanced transformations in sequence."""
        logger.info("Starting comprehensive feature engineering pipeline...")
        
        original_cols = len(df.columns)
        
        # Apply transformations in logical order
        df = self.create_temporal_features(df)
        df = self.create_rfm_features(df)
        df = self.create_product_features(df)
        df = self.create_customer_behavior_features(df)
        df = self.create_market_basket_features(df)
        df = self.create_anomaly_detection_features(df)
        df = self.create_seasonality_features(df)
        df = self.perform_customer_segmentation(df)
        
        new_cols = len(df.columns)
        logger.info(f"Feature engineering complete: added {new_cols - original_cols} new features")
        
        return df


def create_advanced_features(df: pd.DataFrame) -> pd.DataFrame:
    """Main function to create all advanced features."""
    engineer = AdvancedFeatureEngineer()
    return engineer.apply_all_transformations(df)


def main():
    """Test the advanced feature engineering."""
    # This would typically load data from Silver layer
    print("Advanced Feature Engineering Module loaded successfully")
    print("Available transformations:")
    print("- Temporal features (seasonality, business hours, etc.)")
    print("- RFM analysis (Recency, Frequency, Monetary)")
    print("- Product analytics (popularity, price categories)")
    print("- Customer behavior analysis")
    print("- Market basket analysis")
    print("- Anomaly detection features")
    print("- Customer segmentation (K-means)")


if __name__ == "__main__":
    main()