"""Test suite for the data ingestion module."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

from src.models import RawDataModel


class TestRawDataModel:
    """Test cases for RawDataModel validation."""
    
    def test_valid_record(self) -> None:
        """Test validation of a valid record."""
        data = {
            "user_id": 1,
            "order_id": "ORD-001",
            "product_id": "PROD-001",
            "product_name": "Test Product",
            "category": "Electronics",
            "price": 99.99,
            "quantity": 2,
            "order_date": "2024-01-15",
            "status": "completed"
        }
        
        model = RawDataModel(**data)
        
        assert model.user_id == 1
        assert model.order_id == "ORD-001"
        assert model.price == 99.99
    
    def test_invalid_user_id(self) -> None:
        """Test validation rejects negative user_id."""
        data = {
            "user_id": -1,
            "order_id": "ORD-001",
            "product_id": "PROD-001",
            "price": 99.99,
            "quantity": 2,
            "order_date": "2024-01-15"
        }
        
        with pytest.raises(ValueError):
            RawDataModel(**data)
    
    def test_invalid_quantity(self) -> None:
        """Test validation rejects zero quantity."""
        data = {
            "user_id": 1,
            "order_id": "ORD-001",
            "product_id": "PROD-001",
            "price": 99.99,
            "quantity": 0,
            "order_date": "2024-01-15"
        }
        
        with pytest.raises(ValueError):
            RawDataModel(**data)
    
    def test_invalid_price(self) -> None:
        """Test validation rejects negative price."""
        data = {
            "user_id": 1,
            "order_id": "ORD-001",
            "product_id": "PROD-001",
            "price": -10.00,
            "quantity": 1,
            "order_date": "2024-01-15"
        }
        
        with pytest.raises(ValueError):
            RawDataModel(**data)
    
    def test_optional_fields(self) -> None:
        """Test that optional fields can be None."""
        data = {
            "user_id": 1,
            "order_id": "ORD-001",
            "product_id": "PROD-001",
            "price": 99.99,
            "quantity": 1,
            "order_date": "2024-01-15"
        }
        
        model = RawDataModel(**data)
        
        assert model.product_name is None
        assert model.category is None
        assert model.status is None


class TestDataIngestion:
    """Test cases for DataIngestion class."""
    
    @patch('src.ingestion.MongoClient')
    def test_initialization(self, mock_mongo_client: Mock) -> None:
        """Test DataIngestion initialization."""
        mock_instance = MagicMock()
        mock_mongo_client.return_value = mock_instance
        
        from src.ingestion import DataIngestion
        ingestion = DataIngestion()
        
        assert ingestion.client is not None
        mock_mongo_client.assert_called_once()
    
    @patch('src.ingestion.MongoClient')
    def test_validate_and_chunk_data_valid_records(self, mock_mongo_client: Mock) -> None:
        """Test validation with valid records."""
        mock_instance = MagicMock()
        mock_mongo_client.return_value = mock_instance
        
        from src.ingestion import DataIngestion
        ingestion = DataIngestion()
        
        # Create valid test data
        test_data = pd.DataFrame([
            {
                "user_id": 1,
                "order_id": "ORD-001",
                "product_id": "PROD-001",
                "product_name": "Test Product",
                "category": "Electronics",
                "price": 99.99,
                "quantity": 2,
                "order_date": "2024-01-15",
                "status": "completed"
            },
            {
                "user_id": 2,
                "order_id": "ORD-002",
                "product_id": "PROD-002",
                "product_name": "Another Product",
                "category": "Clothing",
                "price": 49.99,
                "quantity": 1,
                "order_date": "2024-01-16",
                "status": "pending"
            }
        ])
        
        chunks = list(ingestion.validate_and_chunk_data(test_data, chunk_size=10))
        
        assert len(chunks) == 1  # All in one chunk
        assert len(chunks[0]) == 2  # Both records valid
        assert chunks[0][0]["user_id"] == 1
        assert chunks[0][1]["user_id"] == 2
    
    @patch('src.ingestion.MongoClient')
    def test_validate_and_chunk_data_invalid_records(self, mock_mongo_client: Mock) -> None:
        """Test validation filters out invalid records."""
        mock_instance = MagicMock()
        mock_mongo_client.return_value = mock_instance
        
        from src.ingestion import DataIngestion
        ingestion = DataIngestion()
        
        # Create test data with one invalid record (negative user_id)
        test_data = pd.DataFrame([
            {
                "user_id": -1,  # Invalid
                "order_id": "ORD-001",
                "product_id": "PROD-001",
                "price": 99.99,
                "quantity": 2,
                "order_date": "2024-01-15"
            },
            {
                "user_id": 2,  # Valid
                "order_id": "ORD-002",
                "product_id": "PROD-002",
                "price": 49.99,
                "quantity": 1,
                "order_date": "2024-01-16"
            }
        ])
        
        chunks = list(ingestion.validate_and_chunk_data(test_data, chunk_size=10))
        
        # Should have 1 chunk with 1 valid record
        assert len(chunks) == 1
        assert len(chunks[0]) == 1
        assert chunks[0][0]["user_id"] == 2
    
    @patch('src.ingestion.MongoClient')
    def test_validate_and_chunk_data_all_invalid(self, mock_mongo_client: Mock) -> None:
        """Test validation with all invalid records."""
        mock_instance = MagicMock()
        mock_mongo_client.return_value = mock_instance
        
        from src.ingestion import DataIngestion
        ingestion = DataIngestion()
        
        # Create test data with all invalid records
        test_data = pd.DataFrame([
            {
                "user_id": -1,
                "order_id": "ORD-001",
                "product_id": "PROD-001",
                "price": 99.99,
                "quantity": 2,
                "order_date": "2024-01-15"
            }
        ])
        
        chunks = list(ingestion.validate_and_chunk_data(test_data, chunk_size=10))
        
        # Should have no chunks (no valid records)
        assert len(chunks) == 0

    @patch('src.ingestion.MongoClient')
    def test_chunking_behavior(self, mock_mongo_client: Mock) -> None:
        """Test that data is properly chunked."""
        mock_instance = MagicMock()
        mock_mongo_client.return_value = mock_instance
        
        from src.ingestion import DataIngestion
        ingestion = DataIngestion()
        
        # Create 5 valid records
        test_data = pd.DataFrame([
            {
                "user_id": i,
                "order_id": f"ORD-{i:03d}",
                "product_id": f"PROD-{i:03d}",
                "price": 10.00 * i,
                "quantity": 1,
                "order_date": "2024-01-15"
            }
            for i in range(1, 6)  # 5 records with user_id 1-5
        ])
        
        # Chunk size of 2 should produce 3 chunks (2, 2, 1)
        chunks = list(ingestion.validate_and_chunk_data(test_data, chunk_size=2))
        
        assert len(chunks) == 3
        assert len(chunks[0]) == 2
        assert len(chunks[1]) == 2
        assert len(chunks[2]) == 1
