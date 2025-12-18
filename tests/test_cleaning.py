"""Test suite for the data cleaning module."""

import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np


class TestDataCleaning:
    """Test cases for DataCleaning class."""
    
    @pytest.fixture
    def sample_raw_data(self) -> pd.DataFrame:
        """Create sample raw data for testing."""
        return pd.DataFrame({
            "user_id": [1, 2, 3, 4, 5],
            "order_id": ["ORD-001", "ORD-002", "ORD-003", "ORD-004", "ORD-005"],
            "product_id": ["PROD-001", "PROD-002", "PROD-003", "PROD-004", "PROD-005"],
            "product_name": ["Laptop", "Mouse", "Chair", "T-Shirt", "Watch"],
            "category": ["electronics", "accessories", "furniture", "clothing", "accessories"],
            "price": [999.99, 29.99, 199.99, 25.00, 150.00],
            "quantity": [1, 2, 1, 3, 1],
            "order_date": ["2024-01-15", "2024-01-16", "2024-01-17", "2024-01-18", "2024-01-19"],
            "status": ["completed", "PENDING", "canceled", "refunded", "complete"],
        })
    
    @patch('src.cleaning.MongoClient')
    def test_initialization(self, mock_mongo: MagicMock) -> None:
        """Test DataCleaning initialization."""
        from src.cleaning import DataCleaning
        
        cleaner = DataCleaning()
        assert cleaner.client is not None
        mock_mongo.assert_called_once()
    
    @patch('src.cleaning.MongoClient')
    def test_clean_data_removes_duplicates(self, mock_mongo: MagicMock) -> None:
        """Test that duplicate rows are removed."""
        from src.cleaning import DataCleaning
        
        cleaner = DataCleaning()
        
        # Create data with duplicates
        df = pd.DataFrame({
            "user_id": [1, 1, 2],
            "order_id": ["ORD-001", "ORD-001", "ORD-002"],
            "product_id": ["PROD-001", "PROD-001", "PROD-002"],
            "product_name": ["Laptop", "Laptop", "Mouse"],
            "category": ["Electronics", "Electronics", "Accessories"],
            "price": [999.99, 999.99, 29.99],
            "quantity": [1, 1, 2],
            "order_date": ["2024-01-15", "2024-01-15", "2024-01-16"],
            "status": ["completed", "completed", "pending"],
        })
        
        cleaned = cleaner.clean_data(df)
        assert len(cleaned) == 2  # Duplicate removed
    
    @patch('src.cleaning.MongoClient')
    def test_clean_data_normalizes_text(self, mock_mongo: MagicMock) -> None:
        """Test that text fields are normalized."""
        from src.cleaning import DataCleaning
        
        cleaner = DataCleaning()
        
        df = pd.DataFrame({
            "user_id": [1],
            "order_id": ["ORD-001"],
            "product_id": ["PROD-001"],
            "product_name": ["  LAPTOP  "],
            "category": ["electronics"],
            "price": [999.99],
            "quantity": [1],
            "order_date": ["2024-01-15"],
            "status": ["COMPLETED"],
        })
        
        cleaned = cleaner.clean_data(df)
        row = cleaned.iloc[0]
        
        assert row["product_name"] == "LAPTOP"  # Trimmed
        assert row["category"] == "Electronics"  # Title case
        assert row["status"] == "completed"  # Lowercase
    
    @patch('src.cleaning.MongoClient')
    def test_clean_data_standardizes_status(self, mock_mongo: MagicMock) -> None:
        """Test that status values are standardized."""
        from src.cleaning import DataCleaning
        
        cleaner = DataCleaning()
        
        df = pd.DataFrame({
            "user_id": [1, 2, 3, 4, 5],
            "order_id": ["ORD-001", "ORD-002", "ORD-003", "ORD-004", "ORD-005"],
            "product_id": ["P1", "P2", "P3", "P4", "P5"],
            "product_name": ["A", "B", "C", "D", "E"],
            "category": ["Cat1", "Cat2", "Cat3", "Cat4", "Cat5"],
            "price": [10.0, 20.0, 30.0, 40.0, 50.0],
            "quantity": [1, 1, 1, 1, 1],
            "order_date": ["2024-01-15"] * 5,
            "status": ["complete", "PENDING", "canceled", "refunded", "invalid"],
        })
        
        cleaned = cleaner.clean_data(df)
        statuses = cleaned["status"].tolist()
        
        assert "completed" in statuses
        assert "pending" in statuses
        assert "cancelled" in statuses
        assert "returned" in statuses
        assert "unknown" in statuses
    
    @patch('src.cleaning.MongoClient')
    def test_clean_data_fills_missing_values(self, mock_mongo: MagicMock) -> None:
        """Test that missing values are filled with defaults."""
        from src.cleaning import DataCleaning
        
        cleaner = DataCleaning()
        
        df = pd.DataFrame({
            "user_id": [1],
            "order_id": ["ORD-001"],
            "product_id": ["PROD-001"],
            "product_name": [None],
            "category": [None],
            "price": [100.0],
            "quantity": [1],
            "order_date": ["2024-01-15"],
            "status": [None],
        })
        
        cleaned = cleaner.clean_data(df)
        row = cleaned.iloc[0]
        
        assert row["product_name"] == "Unknown Product"
        assert row["category"] == "Uncategorized"
        assert row["status"] == "unknown"
    
    @patch('src.cleaning.MongoClient')
    def test_clean_data_filters_invalid_prices(self, mock_mongo: MagicMock) -> None:
        """Test that negative prices are filtered out."""
        from src.cleaning import DataCleaning
        
        cleaner = DataCleaning()
        
        df = pd.DataFrame({
            "user_id": [1, 2],
            "order_id": ["ORD-001", "ORD-002"],
            "product_id": ["PROD-001", "PROD-002"],
            "product_name": ["A", "B"],
            "category": ["Cat1", "Cat2"],
            "price": [-10.0, 50.0],
            "quantity": [1, 1],
            "order_date": ["2024-01-15", "2024-01-16"],
            "status": ["completed", "completed"],
        })
        
        cleaned = cleaner.clean_data(df)
        assert len(cleaned) == 1
        assert cleaned.iloc[0]["price"] > 0
    
    @patch('src.cleaning.MongoClient')
    def test_clean_data_adds_derived_columns(self, mock_mongo: MagicMock) -> None:
        """Test that derived columns are added."""
        from src.cleaning import DataCleaning
        
        cleaner = DataCleaning()
        
        df = pd.DataFrame({
            "user_id": [1],
            "order_id": ["ORD-001"],
            "product_id": ["PROD-001"],
            "product_name": ["Laptop"],
            "category": ["Electronics"],
            "price": [100.0],
            "quantity": [2],
            "order_date": ["2024-03-15"],  # Friday
            "status": ["completed"],
        })
        
        cleaned = cleaner.clean_data(df)
        row = cleaned.iloc[0]
        
        assert row["total_amount"] == 200.0
        assert row["year"] == 2024
        assert row["month"] == 3
        assert row["day_of_week"] == "Friday"
    
    @patch('src.cleaning.MongoClient')
    def test_clean_data_filters_zero_quantity(self, mock_mongo: MagicMock) -> None:
        """Test that zero quantity rows are filtered."""
        from src.cleaning import DataCleaning
        
        cleaner = DataCleaning()
        
        df = pd.DataFrame({
            "user_id": [1, 2],
            "order_id": ["ORD-001", "ORD-002"],
            "product_id": ["PROD-001", "PROD-002"],
            "product_name": ["A", "B"],
            "category": ["Cat1", "Cat2"],
            "price": [10.0, 50.0],
            "quantity": [0, 1],
            "order_date": ["2024-01-15", "2024-01-16"],
            "status": ["completed", "completed"],
        })
        
        cleaned = cleaner.clean_data(df)
        assert len(cleaned) == 1
        assert cleaned.iloc[0]["quantity"] > 0
