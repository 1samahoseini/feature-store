"""
Database migration script
Handles PostgreSQL to CockroachDB migration
"""

import asyncio
import argparse
import sys
from datetime import datetime

import asyncpg
import structlog

from src.pipelines.migration import DatabaseMigration

logger = structlog.get_logger(__name__)


async def main():
    parser = argparse.ArgumentParser(description='Migrate feature store database')
    parser.add_argument('--source-url', required=True, help='Source PostgreSQL connection URL')
    parser.add_argument('--target-url', required=True, help='Target CockroachDB connection URL')
    parser.add_argument('--validate-only', action='store_true', help='Only validate migration prerequisites')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for data migration')
    parser.add_argument('--auto-switchover', action='store_true', help='Automatically switch to new database after migration')
    parser.add_argument('--test', action='store_true', help='Run migration test (dry run)')
    parser.add_argument('--parallel-tables', action='store_true', help='Migrate tables in parallel')
    
    args = parser.parse_args()
    
    migration = DatabaseMigration()
    
    try:
        # Initialize connections
        logger.info("Initializing database connections...")
        await migration.init_connections(args.target_url)
        
        # Update source URL
        migration.pg_pool = await asyncpg.create_pool(args.source_url)  # noqa: F821
        
        logger.info("Database connections established")
        
        # Validate prerequisites
        logger.info("Validating migration prerequisites...")
        validation_results = await migration.validate_migration_prerequisites()
        
        print("\n" + "="*50)
        print("MIGRATION VALIDATION RESULTS")
        print("="*50)
        
        for check_name, check_result in validation_results['checks'].items():
            status = check_result.get('status', 'unknown')
            print(f"{check_name.upper()}: {status.upper()}")
            
            if status == 'fail':
                error = check_result.get('error', 'Unknown error')
                print(f"  Error: {error}")
            elif 'total_records' in check_result:
                print(f"  Records: {check_result['total_records']:,}")
        
        print(f"\nREADY FOR MIGRATION: {'YES' if validation_results['ready_for_migration'] else 'NO'}")
        
        if args.validate_only:
            return
        
        if not validation_results['ready_for_migration']:
            logger.error("Migration prerequisites not met. Aborting migration.")
            sys.exit(1)
        
        # Configure migration
        migration_config = {
            'batch_size': args.batch_size,
            'auto_switchover': args.auto_switchover,
            'parallel_tables': args.parallel_tables,
            'validation_enabled': True,
            'test_mode': args.test
        }
        
        if args.test:
            logger.info("Running migration test (dry run)...")
        else:
            logger.info("Starting database migration...")
            
            # Confirmation prompt
            response = input("\nProceed with migration from PostgreSQL to CockroachDB? (yes/no): ")
            if response.lower() != 'yes':
                logger.info("Migration cancelled by user")
                return
        
        # Execute migration
        datetime.utcnow()
        migration_results = await migration.run_migration(migration_config)
        datetime.utcnow()
        
        # Display results
        print("\n" + "="*50)
        print("MIGRATION RESULTS")
        print("="*50)
        
        print(f"Status: {migration_results['status'].upper()}")
        print(f"Duration: {migration_results.get('duration_seconds', 0):.2f} seconds")
        print(f"Migration ID: {migration_results.get('migration_id', 'N/A')}")
        
        if 'phases' in migration_results:
            phases = migration_results['phases']
            
            for phase_name, phase_result in phases.items():
                print(f"\n{phase_name.upper()} PHASE:")
                print(f"  Status: {phase_result.get('status', 'unknown').upper()}")
                print(f"  Duration: {phase_result.get('duration_seconds', 0):.2f} seconds")
                
                if phase_name == 'data' and 'tables' in phase_result:
                    total_records = 0
                    for table, table_stats in phase_result['tables'].items():
                        records = table_stats.get('records_migrated', 0)
                        duration = table_stats.get('duration_seconds', 0)
                        rate = records / duration if duration > 0 else 0
                        print(f"    {table}: {records:,} records ({rate:.0f} records/sec)")
                        total_records += records
                    print(f"  Total Records Migrated: {total_records:,}")
                
                if 'error' in phase_result:
                    print(f"  Error: {phase_result['error']}")
        
        if migration_results['status'] == 'completed':
            logger.info("Migration completed successfully!")
            
            if migration_config.get('auto_switchover'):
                print("\nSWITCHOVER COMPLETED")
                print("Application is now using CockroachDB")
            else:
                print("\nMANUAL SWITCHOVER REQUIRED")
                print("Update application configuration to use CockroachDB connection string:")
                print(f"  {args.target_url}")
        else:
            logger.error(f"Migration failed: {migration_results.get('error', 'Unknown error')}")
            
            print("\nROLLBACK INSTRUCTIONS:")
            print("1. Ensure application is still using PostgreSQL")
            print("2. Verify PostgreSQL data integrity")
            print("3. Clean up any partial CockroachDB data if needed")
            print("4. Review migration logs for specific error details")
    
    except Exception as e:
        logger.error(f"Migration script failed: {e}")
        sys.exit(1)
    
    finally:
        # Cleanup connections
        await migration.close_connections()


if __name__ == "__main__":
    asyncio.run(main())