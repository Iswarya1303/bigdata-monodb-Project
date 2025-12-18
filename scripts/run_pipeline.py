"""Main pipeline execution script."""

import sys
from pathlib import Path
from loguru import logger

from src.config import settings
from src.ingestion import DataIngestion
from src.cleaning import DataCleaning
from src.aggregation import DataAggregation
from src.models import PipelineMetrics


def setup_logging() -> None:
    """Configure logging for the pipeline."""
    logger.remove()
    logger.add(
        sys.stderr,
        level=settings.log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>"
    )
    logger.add(
        settings.log_file,
        rotation="100 MB",
        retention="10 days",
        level=settings.log_level,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function} - {message}"
    )


def print_metrics(metrics: PipelineMetrics) -> None:
    """Print pipeline metrics in a formatted way."""
    logger.info("=" * 60)
    logger.info(f"Stage: {metrics.stage.upper()}")
    logger.info(f"Records Processed: {metrics.records_processed:,}")
    logger.info(f"Records Failed: {metrics.records_failed:,}")
    logger.info(f"Success Rate: {metrics.success_rate:.2f}%")
    logger.info(f"Execution Time: {metrics.execution_time_seconds:.2f} seconds")
    logger.info(f"Timestamp: {metrics.timestamp}")
    logger.info("=" * 60)


def main() -> None:
    """Execute the complete data pipeline."""
    setup_logging()
    
    logger.info("Starting MongoDB Sharded Cluster Pipeline")
    logger.info(f"MongoDB URI: {settings.mongodb_uri}")
    logger.info(f"Database: {settings.mongodb_database}")
    
    all_metrics = []
    
    try:
        # Stage 1: Data Ingestion
        logger.info("\n" + "="*60)
        logger.info("STAGE 1: DATA INGESTION")
        logger.info("="*60)
        
        ingestion = DataIngestion()
        
        # Setup sharding
        logger.info("Setting up sharding...")
        ingestion.setup_sharding()
        
        # Ingest data
        data_file = Path("data/raw_data.csv")
        if not data_file.exists():
            logger.error(f"Data file not found: {data_file}")
            logger.info("Please place your dataset in data/raw_data.csv")
            logger.info("Or run: uv run python scripts/generate_data.py")
            return
        
        ingestion_metrics = ingestion.ingest_from_csv(data_file)
        print_metrics(ingestion_metrics)
        all_metrics.append(ingestion_metrics)
        
        # Show row count and schema
        row_count = ingestion.get_row_count()
        logger.info(f"\nTotal rows in raw collection: {row_count:,}")
        
        schema_info = ingestion.get_schema_info()
        logger.info(f"Number of fields: {schema_info.get('field_count', 0)}")
        logger.info(f"Fields: {', '.join(schema_info.get('fields', []))}")
        
        ingestion.close()
        
        # Stage 2: Data Cleaning
        logger.info("\n" + "="*60)
        logger.info("STAGE 2: DATA CLEANING")
        logger.info("="*60)
        
        cleaner = DataCleaning()
        cleaning_metrics = cleaner.run_cleaning_pipeline()
        print_metrics(cleaning_metrics)
        all_metrics.append(cleaning_metrics)
        
        # Show data quality report
        logger.info("\nGenerating data quality report...")
        quality_report = cleaner.get_data_quality_report()
        logger.info(f"Clean collection row count: {quality_report['total_rows']:,}")
        logger.info(f"Numeric stats: {quality_report['numeric_stats']}")
        
        cleaner.close()
        
        # Stage 3: Data Aggregation
        logger.info("\n" + "="*60)
        logger.info("STAGE 3: DATA AGGREGATION")
        logger.info("="*60)
        
        aggregator = DataAggregation()
        aggregation_metrics = aggregator.run_aggregation_pipeline()
        print_metrics(aggregation_metrics)
        all_metrics.append(aggregation_metrics)
        
        # Show summary stats
        logger.info("\nGenerating summary statistics...")
        summary = aggregator.get_summary_stats()
        for key, value in summary.items():
            logger.info(f"{key}: {value}")
        
        aggregator.close()
        
        # Final Summary
        logger.info("\n" + "="*60)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("="*60)
        
        total_time = sum(m.execution_time_seconds for m in all_metrics)
        total_processed = sum(m.records_processed for m in all_metrics)
        total_failed = sum(m.records_failed for m in all_metrics)
        
        logger.info(f"Total Execution Time: {total_time:.2f} seconds")
        logger.info(f"Total Records Processed: {total_processed:,}")
        logger.info(f"Total Records Failed: {total_failed:,}")
        
        if total_processed + total_failed > 0:
            success_rate = (total_processed / (total_processed + total_failed)) * 100
            logger.info(f"Overall Success Rate: {success_rate:.2f}%")
        
        logger.info("\nPipeline completed successfully!")
        logger.info("\nNext steps:")
        logger.info("1. Run the dashboard: uv run streamlit run dashboard/app.py")
        logger.info("2. View aggregated data in MongoDB collections:")
        logger.info(f"   - {settings.agg_collection}_category")
        logger.info(f"   - {settings.agg_collection}_month")
        logger.info(f"   - {settings.agg_collection}_status")
        logger.info(f"   - {settings.agg_collection}_user")
        logger.info(f"   - {settings.agg_collection}_day_of_week")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
