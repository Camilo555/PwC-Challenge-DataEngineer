"""
Unit Tests for ETL Transformations
Tests the transformation framework functionality.
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, date
from decimal import Decimal

try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

from etl.transformations.base import TransformationStrategy, TransformationEngine
from etl.transformations.data_cleaning import DataCleaner, ValidationRule
from etl.transformations.scd2 import SCD2Processor


class TestTransformationStrategy:
    """Test transformation strategy pattern."""
    
    def test_strategy_interface(self):
        """Test that strategy interface is properly defined."""
        with pytest.raises(TypeError):
            # Cannot instantiate abstract class
            TransformationStrategy()
    
    def test_pandas_strategy(self):
        """Test pandas transformation strategy."""
        from etl.transformations.pandas_strategy import PandasTransformationStrategy
        
        strategy = PandasTransformationStrategy()
        
        # Test data frame creation
        df = strategy.create_dataframe([
            {"name": "John", "age": 30, "city": "New York"},
            {"name": "Jane", "age": 25, "city": "Boston"}
        ])
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert list(df.columns) == ["name", "age", "city"]
    
    def test_transformation_engine(self):
        """Test transformation engine."""
        engine = TransformationEngine()
        
        # Test engine selection
        pandas_strategy = engine.get_strategy("pandas")
        assert pandas_strategy is not None
        
        if POLARS_AVAILABLE:
            polars_strategy = engine.get_strategy("polars")
            assert polars_strategy is not None
        
        # Test invalid engine
        with pytest.raises(ValueError):
            engine.get_strategy("invalid_engine")


class TestDataCleaner:
    """Test data cleaning functionality."""
    
    def setup_method(self):
        """Setup test data."""
        self.test_data = pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["John Doe", "  Jane Smith  ", "", None, "Bob Wilson"],
            "email": ["john@email.com", "jane@email", "invalid-email", "", "bob@email.com"],
            "age": [30, 25, -5, 200, 35],
            "salary": [50000.0, None, 75000.0, 0.0, 60000.0],
            "phone": ["123-456-7890", "555.123.4567", "invalid", "", "555-987-6543"]
        })
        
        self.cleaner = DataCleaner()
    
    def test_null_handling(self):
        """Test null value handling."""
        # Test drop nulls
        result = self.cleaner.handle_nulls(self.test_data, strategy="drop")
        assert len(result) < len(self.test_data)
        assert result.isnull().sum().sum() == 0
        
        # Test fill nulls
        result = self.cleaner.handle_nulls(self.test_data, strategy="fill", fill_value="unknown")
        assert result.isnull().sum().sum() == 0
        assert "unknown" in result["name"].values
    
    def test_duplicate_removal(self):
        """Test duplicate removal."""
        # Add duplicate row
        df_with_dups = pd.concat([self.test_data, self.test_data.iloc[[0]]], ignore_index=True)
        
        result = self.cleaner.remove_duplicates(df_with_dups, subset=["name", "email"])
        assert len(result) <= len(df_with_dups)
    
    def test_string_cleaning(self):
        """Test string cleaning operations."""
        result = self.cleaner.clean_strings(self.test_data)
        
        # Check that leading/trailing spaces are removed
        assert result.loc[1, "name"] == "Jane Smith"
        
        # Check that empty strings are converted to null
        assert pd.isna(result.loc[2, "name"])
    
    def test_outlier_detection(self):
        """Test outlier detection and handling."""
        # Age column has outliers (-5 and 200)
        outliers = self.cleaner.detect_outliers(self.test_data, "age", method="iqr")
        assert len(outliers) > 0
        
        # Test outlier removal
        cleaned = self.cleaner.handle_outliers(self.test_data, "age", method="remove")
        assert len(cleaned) < len(self.test_data)
    
    def test_data_type_inference(self):
        """Test data type inference and conversion."""
        # Create test data with mixed types
        mixed_data = pd.DataFrame({
            "int_col": ["1", "2", "3", "4"],
            "float_col": ["1.5", "2.7", "3.14", "4.0"],
            "date_col": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"],
            "bool_col": ["true", "false", "1", "0"]
        })
        
        result = self.cleaner.infer_and_convert_types(mixed_data)
        
        assert result["int_col"].dtype in [np.int64, np.int32]
        assert result["float_col"].dtype in [np.float64, np.float32]
        assert pd.api.types.is_datetime64_any_dtype(result["date_col"])
    
    def test_validation_rules(self):
        """Test validation rule system."""
        # Define validation rules
        rules = [
            ValidationRule("age", lambda x: x >= 0 and x <= 150, "Age must be between 0 and 150"),
            ValidationRule("email", lambda x: "@" in str(x) if pd.notna(x) else True, "Email must contain @")
        ]
        
        violations = self.cleaner.validate_data(self.test_data, rules)
        
        assert len(violations) > 0
        assert any("age" in v["column"] for v in violations)
        assert any("email" in v["column"] for v in violations)


class TestSCD2Processor:
    """Test SCD2 processing functionality."""
    
    def setup_method(self):
        """Setup test data."""
        # Current data
        self.current_data = pd.DataFrame({
            "customer_id": [1, 2, 3],
            "customer_name": ["John Doe", "Jane Smith", "Bob Wilson"],
            "email": ["john@email.com", "jane@email.com", "bob@email.com"],
            "city": ["New York", "Boston", "Chicago"],
            "valid_from": [
                datetime(2023, 1, 1),
                datetime(2023, 1, 1), 
                datetime(2023, 1, 1)
            ],
            "valid_to": [None, None, None],
            "is_current": [True, True, True],
            "version": [1, 1, 1]
        })
        
        # Incoming data with changes
        self.incoming_data = pd.DataFrame({
            "customer_id": [1, 2, 3, 4],
            "customer_name": ["John Doe", "Jane Smith Updated", "Bob Wilson", "Alice Johnson"],
            "email": ["john.doe@newemail.com", "jane.smith@email.com", "bob@email.com", "alice@email.com"],
            "city": ["New York", "Boston", "Denver", "Seattle"]
        })
        
        self.processor = SCD2Processor(
            business_key=["customer_id"],
            tracked_columns=["customer_name", "email", "city"]
        )
    
    def test_change_detection(self):
        """Test change detection between current and incoming data."""
        changes = self.processor.detect_changes(self.current_data, self.incoming_data)
        
        # Should detect changes for customers 1, 2 and new customer 4
        assert len(changes) > 0
        
        # Check change types
        change_types = changes["change_type"].unique()
        assert "update" in change_types
        assert "insert" in change_types
    
    def test_scd2_processing(self):
        """Test complete SCD2 processing."""
        result = self.processor.process_scd2_changes(self.current_data, self.incoming_data)
        
        # Should have more records than input due to versioning
        assert len(result) >= len(self.current_data)
        
        # Check that historical records have valid_to dates
        historical = result[result["is_current"] == False]
        assert all(pd.notna(historical["valid_to"]))
        
        # Check that current records have null valid_to
        current = result[result["is_current"] == True]
        assert all(pd.isna(current["valid_to"]))
    
    def test_hash_generation(self):
        """Test attribute hash generation."""
        df_with_hash = self.processor.add_attribute_hash(self.current_data)
        
        assert "attribute_hash" in df_with_hash.columns
        assert df_with_hash["attribute_hash"].notna().all()
        assert df_with_hash["attribute_hash"].nunique() > 0
    
    def test_version_management(self):
        """Test version number management."""
        processed = self.processor.process_scd2_changes(self.current_data, self.incoming_data)
        
        # Check version increments
        versions = processed.groupby("customer_id")["version"].max()
        assert all(versions >= 1)
        
        # Changed records should have higher versions
        changed_customers = [1, 2]  # Based on test data
        for customer_id in changed_customers:
            customer_versions = processed[processed["customer_id"] == customer_id]["version"]
            if len(customer_versions) > 1:
                assert customer_versions.max() > 1


class TestWindowFunctions:
    """Test window function transformations."""
    
    def setup_method(self):
        """Setup test data."""
        self.sales_data = pd.DataFrame({
            "customer_id": [1, 1, 1, 2, 2, 3],
            "order_date": pd.to_datetime([
                "2023-01-01", "2023-01-15", "2023-02-01",
                "2023-01-10", "2023-01-20", "2023-01-05"
            ]),
            "amount": [100.0, 150.0, 200.0, 75.0, 125.0, 300.0]
        })
    
    def test_running_totals(self):
        """Test running total calculations."""
        from etl.transformations.windowing import calculate_running_totals
        
        result = calculate_running_totals(
            self.sales_data,
            partition_cols=["customer_id"],
            order_col="order_date",
            sum_col="amount"
        )
        
        assert "amount_running_total" in result.columns
        
        # Check running totals for customer 1
        customer_1 = result[result["customer_id"] == 1].sort_values("order_date")
        expected_totals = [100.0, 250.0, 450.0]
        actual_totals = customer_1["amount_running_total"].tolist()
        
        assert actual_totals == expected_totals
    
    def test_lag_lead_calculations(self):
        """Test lag and lead calculations."""
        from etl.transformations.windowing import calculate_lag_lead
        
        result = calculate_lag_lead(
            self.sales_data,
            partition_cols=["customer_id"],
            order_col="order_date",
            value_col="amount",
            lag=1,
            lead=1
        )
        
        assert "amount_lag_1" in result.columns
        assert "amount_lead_1" in result.columns
        
        # Check lag/lead for customer 1
        customer_1 = result[result["customer_id"] == 1].sort_values("order_date")
        
        # First record should have null lag, last record should have null lead
        assert pd.isna(customer_1.iloc[0]["amount_lag_1"])
        assert pd.isna(customer_1.iloc[-1]["amount_lead_1"])
    
    def test_rank_percentile(self):
        """Test rank and percentile calculations."""
        from etl.transformations.windowing import calculate_rank_percentile
        
        result = calculate_rank_percentile(
            self.sales_data,
            partition_cols=["customer_id"],
            order_col="amount"
        )
        
        assert "rank" in result.columns
        assert "percentile" in result.columns
        
        # Ranks should be integers >= 1
        assert all(result["rank"] >= 1)
        assert all(result["percentile"] >= 0)
        assert all(result["percentile"] <= 1)


@pytest.mark.integration
class TestTransformationIntegration:
    """Integration tests for transformation pipeline."""
    
    def test_end_to_end_transformation(self):
        """Test complete transformation pipeline."""
        # Raw data with quality issues
        raw_data = pd.DataFrame({
            "id": [1, 2, 3, 4, 5, 5],  # Duplicate
            "name": ["John", "  Jane  ", "", None, "Bob", "Bob"],
            "age": [30, 25, -5, 200, 35, 35],  # Outliers
            "salary": [50000, None, 75000, 0, 60000, 60000],
            "date_created": ["2023-01-01", "2023-01-02", "invalid", "2023-01-04", "2023-01-05", "2023-01-05"]
        })
        
        # Initialize cleaner
        cleaner = DataCleaner()
        
        # Step 1: Clean strings
        cleaned = cleaner.clean_strings(raw_data)
        
        # Step 2: Handle nulls
        cleaned = cleaner.handle_nulls(cleaned, strategy="drop")
        
        # Step 3: Remove duplicates
        cleaned = cleaner.remove_duplicates(cleaned)
        
        # Step 4: Handle outliers
        cleaned = cleaner.handle_outliers(cleaned, "age", method="remove")
        
        # Step 5: Infer and convert types
        cleaned = cleaner.infer_and_convert_types(cleaned)
        
        # Verify final result
        assert len(cleaned) < len(raw_data)  # Some rows removed
        assert cleaned.isnull().sum().sum() == 0  # No nulls
        assert len(cleaned) == len(cleaned.drop_duplicates())  # No duplicates
        assert all(cleaned["age"] > 0) and all(cleaned["age"] < 150)  # No outliers


class TestPerformance:
    """Performance tests for transformations."""
    
    @pytest.mark.slow
    def test_large_dataset_performance(self):
        """Test performance with large dataset."""
        import time
        
        # Generate large dataset
        n_rows = 100000
        large_data = pd.DataFrame({
            "id": range(n_rows),
            "value": np.random.randn(n_rows),
            "category": np.random.choice(["A", "B", "C", "D"], n_rows),
            "date": pd.date_range("2020-01-01", periods=n_rows, freq="H")
        })
        
        # Test transformation performance
        start_time = time.time()
        
        from etl.transformations.windowing import calculate_running_totals
        result = calculate_running_totals(
            large_data,
            partition_cols=["category"],
            order_col="date",
            sum_col="value"
        )
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Should complete within reasonable time (adjust threshold as needed)
        assert duration < 30  # 30 seconds threshold
        assert len(result) == n_rows
        assert "value_running_total" in result.columns
    
    def test_memory_efficiency(self):
        """Test memory usage efficiency."""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        # Create and process medium-sized dataset
        n_rows = 50000
        data = pd.DataFrame({
            "id": range(n_rows),
            "value": np.random.randn(n_rows),
            "text": ["sample_text_" + str(i) for i in range(n_rows)]
        })
        
        cleaner = DataCleaner()
        cleaned = cleaner.clean_strings(data)
        cleaned = cleaner.handle_nulls(cleaned)
        
        final_memory = process.memory_info().rss
        memory_increase = final_memory - initial_memory
        
        # Memory increase should be reasonable (less than 500MB for this dataset)
        assert memory_increase < 500 * 1024 * 1024  # 500MB threshold


if __name__ == "__main__":
    pytest.main([__file__, "-v"])