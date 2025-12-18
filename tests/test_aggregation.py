"""Test suite for the data aggregation module."""

import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime


class TestDataAggregation:
    """Test cases for DataAggregation class."""
    
    @pytest.fixture
    def sample_clean_data(self) -> pd.DataFrame:
        """Create sample cleaned data for aggregation tests."""
        return pd.DataFrame({
            "user_id": [1, 2, 1, 3, 4],
            "order_id": ["ORD-001", "ORD-002", "ORD-003", "ORD-004", "ORD-005"],
            "product_id": ["PROD-001", "PROD-002", "PROD-003", "PROD-004", "PROD-005"],
            "product_name": ["Laptop", "Mouse", "Chair", "T-Shirt", "Jeans"],
            "category": ["Electronics", "Electronics", "Furniture", "Clothing", "Clothing"],
            "price": [999.99, 29.99, 199.99, 25.00, 50.00],
            "quantity": [1, 2, 1, 3, 1],
            "order_date": pd.to_datetime([
                "2024-01-15", "2024-01-16", "2024-02-17", "2024-01-20", "2024-02-25"
            ]),
            "status": ["completed", "completed", "pending", "completed", "cancelled"],
            "total_amount": [999.99, 59.98, 199.99, 75.00, 50.00],
            "year": [2024, 2024, 2024, 2024, 2024],
            "month": [1, 1, 2, 1, 2],
            "day_of_week": ["Monday", "Tuesday", "Saturday", "Saturday", "Sunday"],
        })
    
    @patch('src.aggregation.MongoClient')
    def test_initialization(self, mock_mongo: MagicMock) -> None:
        """Test DataAggregation initialization."""
        from src.aggregation import DataAggregation
        
        aggregator = DataAggregation()
        assert aggregator.client is not None
        mock_mongo.assert_called_once()
    
    @patch('src.aggregation.MongoClient')
    def test_aggregate_by_category(
        self, mock_mongo: MagicMock, sample_clean_data: pd.DataFrame
    ) -> None:
        """Test category aggregation."""
        from src.aggregation import DataAggregation
        
        aggregator = DataAggregation()
        result = aggregator.aggregate_by_category(sample_clean_data)
        
        # Should have 3 categories
        assert len(result) == 3
        
        # Check Electronics aggregation
        electronics = result[result["_id"] == "Electronics"].iloc[0]
        assert electronics["total_orders"] == 2
        assert electronics["unique_customers"] == 2
    
    @patch('src.aggregation.MongoClient')
    def test_aggregate_by_status(
        self, mock_mongo: MagicMock, sample_clean_data: pd.DataFrame
    ) -> None:
        """Test status aggregation."""
        from src.aggregation import DataAggregation
        
        aggregator = DataAggregation()
        result = aggregator.aggregate_by_status(sample_clean_data)
        
        # Should have 3 unique statuses
        assert len(result) == 3
        
        # Check completed orders
        completed = result[result["_id"] == "completed"].iloc[0]
        assert completed["total_orders"] == 3
    
    @patch('src.aggregation.MongoClient')
    def test_aggregate_by_month(
        self, mock_mongo: MagicMock, sample_clean_data: pd.DataFrame
    ) -> None:
        """Test monthly aggregation."""
        from src.aggregation import DataAggregation
        
        aggregator = DataAggregation()
        result = aggregator.aggregate_by_month(sample_clean_data)
        
        # Should have 2 months
        assert len(result) == 2
        
        # Check January 2024
        jan_2024 = result[result["_id"] == "2024-01"].iloc[0]
        assert jan_2024["total_orders"] == 3
    
    @patch('src.aggregation.MongoClient')
    def test_aggregate_by_user(
        self, mock_mongo: MagicMock, sample_clean_data: pd.DataFrame
    ) -> None:
        """Test user aggregation."""
        from src.aggregation import DataAggregation
        
        aggregator = DataAggregation()
        result = aggregator.aggregate_by_user(sample_clean_data)
        
        # Should have 4 unique users
        assert len(result) == 4
        
        # User 1 has 2 orders
        user_1 = result[result["_id"] == "1"].iloc[0]
        assert user_1["total_orders"] == 2
        assert user_1["categories_purchased"] == 2
    
    @patch('src.aggregation.MongoClient')
    def test_aggregate_day_of_week(
        self, mock_mongo: MagicMock, sample_clean_data: pd.DataFrame
    ) -> None:
        """Test day of week aggregation."""
        from src.aggregation import DataAggregation
        
        aggregator = DataAggregation()
        result = aggregator.aggregate_day_of_week(sample_clean_data)
        
        # Check that days are present
        days = result["_id"].tolist()
        assert "Monday" in days
        assert "Tuesday" in days
        assert "Saturday" in days
        assert "Sunday" in days
        
        # Saturday has 2 orders
        saturday = result[result["_id"] == "Saturday"].iloc[0]
        assert saturday["total_orders"] == 2
    
    @patch('src.aggregation.MongoClient')
    def test_revenue_calculation(
        self, mock_mongo: MagicMock, sample_clean_data: pd.DataFrame
    ) -> None:
        """Test that revenue is calculated correctly."""
        from src.aggregation import DataAggregation
        
        aggregator = DataAggregation()
        result = aggregator.aggregate_by_category(sample_clean_data)
        
        # Electronics: 999.99 + 59.98 = 1059.97
        electronics = result[result["_id"] == "Electronics"].iloc[0]
        assert abs(electronics["total_revenue"] - 1059.97) < 0.01
        
        # Clothing: 75.00 + 50.00 = 125.00
        clothing = result[result["_id"] == "Clothing"].iloc[0]
        assert abs(clothing["total_revenue"] - 125.00) < 0.01
    
    @patch('src.aggregation.MongoClient')
    def test_avg_order_value(
        self, mock_mongo: MagicMock, sample_clean_data: pd.DataFrame
    ) -> None:
        """Test average order value calculation."""
        from src.aggregation import DataAggregation
        
        aggregator = DataAggregation()
        result = aggregator.aggregate_by_category(sample_clean_data)
        
        # Electronics avg: (999.99 + 59.98) / 2 = 529.985
        electronics = result[result["_id"] == "Electronics"].iloc[0]
        assert abs(electronics["avg_order_value"] - 529.99) < 0.1
    
    @patch('src.aggregation.MongoClient')
    def test_quantity_sum(
        self, mock_mongo: MagicMock, sample_clean_data: pd.DataFrame
    ) -> None:
        """Test quantity summation."""
        from src.aggregation import DataAggregation
        
        aggregator = DataAggregation()
        result = aggregator.aggregate_by_category(sample_clean_data)
        
        # Electronics: 1 + 2 = 3
        electronics = result[result["_id"] == "Electronics"].iloc[0]
        assert electronics["total_quantity"] == 3
        
        # Clothing: 3 + 1 = 4
        clothing = result[result["_id"] == "Clothing"].iloc[0]
        assert clothing["total_quantity"] == 4
    
    @patch('src.aggregation.MongoClient')
    def test_day_of_week_ordering(
        self, mock_mongo: MagicMock, sample_clean_data: pd.DataFrame
    ) -> None:
        """Test that days are properly ordered."""
        from src.aggregation import DataAggregation
        
        aggregator = DataAggregation()
        result = aggregator.aggregate_day_of_week(sample_clean_data)
        
        # Get the order of days in result
        days = result["_id"].tolist()
        expected_order = ["Monday", "Tuesday", "Saturday", "Sunday"]
        
        # Check that the days appear in correct order
        day_indices = [expected_order.index(d) for d in days]
        assert day_indices == sorted(day_indices)
