#!/usr/bin/env python3
"""
Ultra-Fast Microdata Ingestion Script
Uses the fastest possible insertion methods for maximum performance
"""

import pandas as pd
import json
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
import os
import sys
import io
import time
from pathlib import Path
import numpy as np

# Database Configuration
DB_HOST = "localhost"
DB_NAME = "statathon" 
DB_USER = "postgres" 
DB_PASSWORD = "Suraj@#6708"

# Performance Configuration
BATCH_SIZE = 25000  # Optimized batch size for better memory management
USE_COPY_COMMAND = True  # Use PostgreSQL COPY command (fastest)
USE_BULK_INSERT = True  # Use bulk INSERT statements
USE_TRANSACTION_BATCHING = True  # Batch transactions for better performance
CHUNK_SIZE = 50000  # Process CSV in manageable chunks
RETRY_ATTEMPTS = 3  # Number of retry attempts for failed operations

# CSV Configuration
MICRODATA_CSV_DIR = '../Data_Injection/hces_microdata_csvs'

def get_db_connection():
    """Get optimized database connection with performance settings"""
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    # Set session-level optimizations (only the ones that can be changed)
    with conn.cursor() as cur:
        cur.execute("SET synchronous_commit TO off")
        cur.execute("SET work_mem TO '256MB'")
        cur.execute("SET maintenance_work_mem TO '256MB'")
        cur.execute("SET temp_buffers TO '64MB'")
        cur.execute("SET effective_cache_size TO '1GB'")
    return conn

def ultra_fast_copy_insert(cur, data_records, table_name):
    """
    Ultra-fast insertion using PostgreSQL COPY command.
    This is the fastest method for large datasets.
    """
    try:
        # Create a StringIO buffer
        buffer = io.StringIO()
        
        for record in data_records:
            # Convert record to tab-separated format
            row_values = []
            for value in record:
                if value is None:
                    row_values.append('\\N')
                else:
                    # Escape special characters
                    str_value = str(value).replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n')
                    row_values.append(str_value)
            
            buffer.write('\t'.join(row_values) + '\n')
        
        buffer.seek(0)
        
        # Execute COPY command
        cur.copy_from(
            buffer,
            table_name,
            columns=['survey_id', 'level_id', 'unit_identifier', 'data_payload'],
            sep='\t',
            null='\\N'
        )
        
        return True
    except Exception as e:
        print(f"COPY command error: {e}")
        return False

def bulk_insert_with_execute_values(cur, data_records, batch_size=50000):
    """
    Fast bulk insertion using execute_values with optimized settings.
    """
    try:
        query = """
            INSERT INTO survey_data (survey_id, level_id, unit_identifier, data_payload)
            VALUES %s
            ON CONFLICT DO NOTHING;
        """
        
        # Process in batches for optimal performance
        for i in range(0, len(data_records), batch_size):
            batch = data_records[i:i + batch_size]
            execute_values(cur, query, batch, page_size=batch_size)
        
        return True
    except Exception as e:
        print(f"Bulk insert error: {e}")
        return False

def process_csv_chunk_optimized(chunk_df, variable_schema, common_identifiers, asi_survey_id, level_id):
    """
    Optimized chunk processing using vectorized operations.
    """
    processed_records = []
    
    # Convert column names to uppercase once
    chunk_df.columns = [col.upper() for col in chunk_df.columns]
    
    # Vectorized processing for better performance
    for _, row in chunk_df.iterrows():
        current_record_data = {}
        unit_id_parts = []
        
        for var_def in variable_schema:
            var_name = var_def['name'].upper()
            mapped_type = var_def['type']
            is_common_id = var_def['is_common_id']
            
            raw_value = row.get(var_name)
            
            if pd.isna(raw_value) or str(raw_value).strip() == '':
                processed_value = None
            else:
                try:
                    if mapped_type == 'INTEGER':
                        processed_value = int(float(raw_value))
                    elif mapped_type == 'NUMERIC':
                        processed_value = float(raw_value)
                    else:
                        processed_value = str(raw_value).strip()
                except (ValueError, TypeError):
                    processed_value = str(raw_value).strip()
            
            current_record_data[var_name] = processed_value
            
            if is_common_id and processed_value is not None:
                unit_id_parts.append(str(processed_value))
        
        # Check if we have all required common identifiers
        if len(unit_id_parts) == len(common_identifiers) and all(p != '' for p in unit_id_parts):
            unit_identifier = "_".join(unit_id_parts)
            processed_records.append((
                asi_survey_id, 
                level_id, 
                unit_identifier, 
                json.dumps(current_record_data)
            ))
    
    return processed_records

def ingest_microdata_ultra_fast():
    """
    Ultra-fast microdata ingestion using the fastest possible methods.
    """
    conn = None
    cur = None
    start_time = time.time()
    
    try:
        print("Starting Ultra-Fast Microdata Ingestion...")
        print(f"Performance Settings:")
        print(f"   - Batch Size: {BATCH_SIZE:,}")
        print(f"   - Chunk Size: {CHUNK_SIZE:,}")
        print(f"   - Use COPY Command: {USE_COPY_COMMAND}")
        print(f"   - Use Bulk Insert: {USE_BULK_INSERT}")
        print(f"   - Transaction Batching: {USE_TRANSACTION_BATCHING}")
        print()
        
        # Get database connection
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get survey ID
        cur.execute("SELECT survey_id FROM surveys WHERE survey_name = %s AND survey_year = %s;", ('ASI', 2023))
        result = cur.fetchone()
        if not result:
            print("Error: No survey_id found for ASI 2023")
            sys.exit(1)
        
        asi_survey_id = result[0]
        print(f"ASI 2023 Survey ID: {asi_survey_id}")
        
        # Load metadata schemas
        cur.execute("""
            SELECT level_id, level_name, variable_schema, common_identifiers 
            FROM survey_levels 
            WHERE survey_id = %s;
        """, (asi_survey_id,))
        
        all_level_metadata = {
            row[1]: {
                'level_id': row[0], 
                'variable_schema': row[2], 
                'common_identifiers': row[3]
            }
            for row in cur.fetchall()
        }
        
        print(f"Loaded metadata for {len(all_level_metadata)} levels")
        
        # Check CSV directory
        csv_dir = Path(MICRODATA_CSV_DIR)
        if not csv_dir.exists():
            print(f"CSV directory not found: {csv_dir}")
            sys.exit(1)
        
        csv_files = list(csv_dir.glob('*.csv'))
        if not csv_files:
            print("No CSV files found")
            sys.exit(1)
        
        print(f"Found {len(csv_files)} CSV files")
        
        total_inserted = 0
        total_processed = 0
        
        for csv_file in csv_files:
            file_start_time = time.time()
            print(f"\nProcessing: {csv_file.name}")
            
            # Determine database level mapping
            db_level_name = 'ASI_BLOCK_C'  # Default mapping
            if db_level_name not in all_level_metadata:
                print(f"No metadata found for level: {db_level_name}")
                continue
            
            level_info = all_level_metadata[db_level_name]
            level_id = level_info['level_id']
            variable_schema = level_info['variable_schema']
            common_identifiers = level_info['common_identifiers']
            
            print(f"   -> Mapping to DB Level: {db_level_name} (ID: {level_id})")
            
            try:
                # Read CSV in chunks for memory efficiency
                chunk_iter = pd.read_csv(
                    csv_file, 
                    header=None, 
                    dtype=str, 
                    chunksize=CHUNK_SIZE,
                    engine='c'  # Use C engine for better performance
                )
                
                file_records = 0
                file_inserted = 0
                chunk_count = 0
                
                for chunk in chunk_iter:
                    chunk_count += 1
                    chunk_start = time.time()
                    
                    print(f"   Processing chunk {chunk_count} ({len(chunk):,} records)...")
                    
                    # Process chunk
                    processed_records = process_csv_chunk_optimized(
                        chunk, variable_schema, common_identifiers, asi_survey_id, level_id
                    )
                    
                    if processed_records:
                        # Choose fastest insertion method
                        if USE_COPY_COMMAND and len(processed_records) > 1000:
                            # Use COPY command (fastest)
                            success = ultra_fast_copy_insert(cur, processed_records, 'survey_data')
                        else:
                            # Use bulk insert as fallback
                            success = bulk_insert_with_execute_values(cur, processed_records, BATCH_SIZE)
                        
                        if success:
                            file_inserted += len(processed_records)
                            total_inserted += len(processed_records)
                            
                            # Commit in batches for better performance
                            if USE_TRANSACTION_BATCHING and chunk_count % 5 == 0:
                                conn.commit()
                            
                            chunk_time = time.time() - chunk_start
                            speed = len(processed_records) / chunk_time if chunk_time > 0 else 0
                            print(f"      Inserted {len(processed_records):,} records in {chunk_time:.2f}s ({speed:.0f} records/sec)")
                        else:
                            print(f"      Failed to insert chunk {chunk_count}")
                            conn.rollback()
                            return
                    
                    file_records += len(chunk)
                    total_processed += len(chunk)
                
                # Final commit for this file
                conn.commit()
                
                file_time = time.time() - file_start_time
                print(f"   File completed in {file_time:.2f}s")
                print(f"      Total: {file_records:,}, Inserted: {file_inserted:,}")
                
            except Exception as e:
                print(f"   Error processing {csv_file.name}: {e}")
                conn.rollback()
                continue
        
        # Final commit
        conn.commit()
        
        total_time = time.time() - start_time
        print(f"\nUltra-Fast Ingestion Completed!")
        print(f"Total Records Processed: {total_processed:,}")
        print(f"Total Records Inserted: {total_inserted:,}")
        print(f"Total Time: {total_time:.2f}s")
        print(f"Average Speed: {total_inserted/total_time:.0f} records/second")
        
        if total_processed > 0:
            success_rate = (total_inserted / total_processed) * 100
            print(f"Success Rate: {success_rate:.1f}%")
        
    except Exception as e:
        print(f"Critical error: {e}")
        if conn:
            conn.rollback()
        sys.exit(1)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            print("Database connection closed")

if __name__ == "__main__":
    ingest_microdata_ultra_fast()
