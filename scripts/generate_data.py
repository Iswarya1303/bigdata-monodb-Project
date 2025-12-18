"""Script to generate sample e-commerce data for the pipeline."""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger

# Configuration
DEFAULT_ROWS = 1_000_000
DATA_DIR = Path(__file__).parent.parent / "data"
OUTPUT_FILE = DATA_DIR / "raw_data.csv"

# Data generation parameters
PRODUCTS = {
    "Electronics": ["Laptop", "Smartphone", "Tablet", "Monitor", "Headphones", "Keyboard", "Mouse", "Webcam"],
    "Clothing": ["T-Shirt", "Jeans", "Jacket", "Sneakers", "Dress", "Sweater", "Shorts", "Hat"],
    "Furniture": ["Chair", "Desk", "Table", "Sofa", "Bed", "Bookshelf", "Cabinet", "Lamp"],
    "Accessories": ["Watch", "Bag", "Wallet", "Sunglasses", "Belt", "Scarf", "Jewelry", "Umbrella"],
    "Sports": ["Football", "Basketball", "Tennis Racket", "Yoga Mat", "Dumbbells", "Running Shoes", "Bicycle", "Swimming Goggles"],
}

STATUSES = ["completed", "pending", "cancelled", "returned"]
STATUS_WEIGHTS = [0.7, 0.15, 0.1, 0.05]  # 70% completed, etc.


def setup_logging() -> None:
    """Configure logging."""
    logger.remove()
    logger.add(
        sys.stderr,
        level="INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>"
    )


def generate_data(n_rows: int, seed: int = 42) -> pd.DataFrame:
    """Generate sample e-commerce data.
    
    Args:
        n_rows: Number of rows to generate
        seed: Random seed for reproducibility
        
    Returns:
        DataFrame with generated data
    """
    np.random.seed(seed)
    
    logger.info(f"Generating {n_rows:,} rows of sample data...")
    
    # Generate categories and products
    categories = []
    product_names = []
    
    for _ in range(n_rows):
        category = np.random.choice(list(PRODUCTS.keys()))
        product = np.random.choice(PRODUCTS[category])
        categories.append(category)
        product_names.append(product)
    
    # Generate price ranges based on category
    base_prices = {
        "Electronics": (50, 2000),
        "Clothing": (15, 200),
        "Furniture": (30, 1500),
        "Accessories": (10, 300),
        "Sports": (20, 500),
    }
    
    prices = []
    for cat in categories:
        low, high = base_prices[cat]
        price = np.random.uniform(low, high)
        prices.append(round(price, 2))
    
    # Generate dates across 2024
    start_date = pd.Timestamp("2024-01-01")
    end_date = pd.Timestamp("2024-12-31")
    date_range = (end_date - start_date).days
    
    random_days = np.random.randint(0, date_range, n_rows)
    order_dates = [
        (start_date + pd.Timedelta(days=int(d))).strftime("%Y-%m-%d")
        for d in random_days
    ]
    
    # Create DataFrame
    data = {
        "user_id": np.random.randint(1, 50000, n_rows),
        "order_id": [f"ORD-{i:08d}" for i in range(n_rows)],
        "product_id": [f"PROD-{np.random.randint(1, 5000):05d}" for _ in range(n_rows)],
        "product_name": product_names,
        "category": categories,
        "price": prices,
        "quantity": np.random.randint(1, 10, n_rows),
        "order_date": order_dates,
        "status": np.random.choice(STATUSES, n_rows, p=STATUS_WEIGHTS),
    }
    
    df = pd.DataFrame(data)
    
    # Add some intentional data quality issues for cleaning demo
    # (about 0.5% of data will have issues)
    n_issues = int(n_rows * 0.005)
    
    # Some missing product names
    missing_idx = np.random.choice(df.index, n_issues // 3, replace=False)
    df.loc[missing_idx, "product_name"] = None
    
    # Some missing categories  
    missing_idx = np.random.choice(df.index, n_issues // 3, replace=False)
    df.loc[missing_idx, "category"] = None
    
    logger.info(f"Added {n_issues} intentional data quality issues for cleaning demo")
    
    return df


def save_data(df: pd.DataFrame, output_path: Path) -> None:
    """Save DataFrame to CSV file.
    
    Args:
        df: DataFrame to save
        output_path: Path to output file
    """
    # Create data directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(output_path, index=False)
    
    file_size = output_path.stat().st_size / (1024 * 1024)  # MB
    logger.info(f"Saved {len(df):,} rows to {output_path}")
    logger.info(f"   File size: {file_size:.2f} MB")


def main() -> None:
    """Main function to generate and save sample data."""
    setup_logging()
    
    logger.info("=" * 50)
    logger.info("Sample Data Generator")
    logger.info("=" * 50)
    
    # Parse command line arguments
    n_rows = DEFAULT_ROWS
    if len(sys.argv) > 1:
        try:
            n_rows = int(sys.argv[1])
        except ValueError:
            logger.error(f"Invalid number of rows: {sys.argv[1]}")
            sys.exit(1)
    
    logger.info(f"Target rows: {n_rows:,}")
    logger.info(f"Output file: {OUTPUT_FILE}")
    
    # Generate and save data
    df = generate_data(n_rows)
    save_data(df, OUTPUT_FILE)
    
    # Show summary
    logger.info("\n--- Data Summary ---")
    logger.info(f"Columns: {list(df.columns)}")
    logger.info(f"Categories: {df['category'].dropna().unique().tolist()}")
    logger.info(f"Status distribution:")
    for status, count in df['status'].value_counts().items():
        pct = count / len(df) * 100
        logger.info(f"  - {status}: {count:,} ({pct:.1f}%)")
    
    logger.info("\n" + "=" * 50)
    logger.info("Data generation complete!")
    logger.info("=" * 50)
    logger.info(f"\nNext step: Run the pipeline with:")
    logger.info("  uv run python scripts/run_pipeline.py")


if __name__ == "__main__":
    main()

