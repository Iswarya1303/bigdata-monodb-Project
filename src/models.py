"""Pydantic models for schema validation."""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, validator, field_validator
from decimal import Decimal


class RawDataModel(BaseModel):
    """Schema for raw data validation.
    
    Adjust fields based on your dataset.
    This example uses an e-commerce dataset structure.
    """
    
    user_id: int = Field(..., description="Unique user identifier", gt=0)
    order_id: str = Field(..., description="Order identifier", min_length=1)
    product_id: str = Field(..., description="Product identifier", min_length=1)
    product_name: Optional[str] = Field(None, description="Product name")
    category: Optional[str] = Field(None, description="Product category")
    price: float = Field(..., description="Product price", ge=0)
    quantity: int = Field(..., description="Quantity ordered", gt=0)
    order_date: str = Field(..., description="Order date string")
    status: Optional[str] = Field(None, description="Order status")
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "user_id": 12345,
                "order_id": "ORD-001",
                "product_id": "PROD-ABC",
                "product_name": "Laptop",
                "category": "Electronics",
                "price": 999.99,
                "quantity": 1,
                "order_date": "2024-01-15",
                "status": "completed"
            }
        }
    }


class CleanDataModel(BaseModel):
    """Schema for cleaned data validation."""
    
    user_id: int = Field(..., gt=0)
    order_id: str = Field(..., min_length=1)
    product_id: str = Field(..., min_length=1)
    product_name: str = Field(..., min_length=1)
    category: str = Field(..., min_length=1)
    price: Decimal = Field(..., ge=0, decimal_places=2)
    quantity: int = Field(..., gt=0)
    order_date: datetime = Field(...)
    status: str = Field(..., min_length=1)
    total_amount: Decimal = Field(..., ge=0, decimal_places=2)
    year: int = Field(..., ge=2000, le=2100)
    month: int = Field(..., ge=1, le=12)
    day_of_week: str = Field(...)
    
    @field_validator('status')
    @classmethod
    def validate_status(cls, v: str) -> str:
        """Normalize status values."""
        valid_statuses = {'completed', 'pending', 'cancelled', 'returned'}
        normalized = v.lower().strip()
        if normalized not in valid_statuses:
            return 'unknown'
        return normalized
    
    @field_validator('category')
    @classmethod
    def normalize_category(cls, v: str) -> str:
        """Normalize category names."""
        return v.strip().title()
    
    @field_validator('product_name')
    @classmethod
    def normalize_product_name(cls, v: str) -> str:
        """Normalize product names."""
        return ' '.join(v.strip().split())
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "user_id": 12345,
                "order_id": "ORD-001",
                "product_id": "PROD-ABC",
                "product_name": "Laptop",
                "category": "Electronics",
                "price": "999.99",
                "quantity": 1,
                "order_date": "2024-01-15T10:30:00",
                "status": "completed",
                "total_amount": "999.99",
                "year": 2024,
                "month": 1,
                "day_of_week": "Monday"
            }
        }
    }


class AggregatedDataModel(BaseModel):
    """Schema for aggregated data."""
    
    id: str = Field(..., alias="_id", description="Aggregation key")
    total_orders: int = Field(..., ge=0)
    total_revenue: Decimal = Field(..., ge=0, decimal_places=2)
    avg_order_value: Decimal = Field(..., ge=0, decimal_places=2)
    unique_customers: int = Field(..., ge=0)
    total_quantity: int = Field(..., ge=0)
    
    model_config = {
        "populate_by_name": True,
        "json_schema_extra": {
            "example": {
                "_id": "Electronics",
                "total_orders": 1500,
                "total_revenue": "150000.00",
                "avg_order_value": "100.00",
                "unique_customers": 850,
                "total_quantity": 2000
            }
        }
    }


class PipelineMetrics(BaseModel):
    """Pipeline execution metrics."""
    
    stage: str
    records_processed: int = Field(..., ge=0)
    records_failed: int = Field(default=0, ge=0)
    execution_time_seconds: float = Field(..., ge=0)
    timestamp: datetime = Field(default_factory=datetime.now)
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage."""
        total = self.records_processed + self.records_failed
        if total == 0:
            return 0.0
        return (self.records_processed / total) * 100