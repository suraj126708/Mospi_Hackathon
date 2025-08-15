import pandas as pd
import json
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
import os
import sys
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

DB_HOST = "localhost"
DB_NAME = "statathon" 
DB_USER = "postgres" 
DB_PASSWORD = "Suraj@#6708"

# --- Microdata CSV Files Configuration ---
MICRODATA_CSV_DIR = './hces_microdata_csvs'

# --- CRITICAL MAPPING: CSV Filenames to Database Level Names ---
# This will map any CSV file to ASI_BLOCK_C for now
# You can customize this mapping based on your specific file naming convention
CSV_FILE_TO_DB_LEVEL_NAME = {
    # Default mapping for any CSV file
    'default': 'ASI_BLOCK_C',
}

# --- Optional: Header Row Mapping per CSV File ---
CSV_HEADER_ROW_MAP = {
    'blkC202223.CSV': 0,
   
}

# --- Optional: CSV Delimiter Mapping per CSV File ---
CSV_DELIMITER_MAP = {
    # '_MConverter.eu_LEVEL - 05 ( Sec 5 & 6).csv': '\t', # Example: tab-separated
}

# --- Performance Configuration ---
BATCH_SIZE = 10000  # Increased batch size for better performance
USE_COPY_COMMAND = True  # Use PostgreSQL COPY command for fastest insertion
USE_PARALLEL_PROCESSING = False  # Enable for very large datasets (experimental)
MAX_WORKERS = 4  # Number of parallel workers if parallel processing is enabled

def get_db_connection():
    """Establishes and returns a database connection."""
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def fast_insert_with_copy(cur, data_to_insert, table_name, columns):
    """
    Fastest insertion method using PostgreSQL COPY command.
    This is significantly faster than INSERT statements for large datasets.
    """
    try:
        # Create a StringIO buffer with the data
        buffer = io.StringIO()
        
        for row in data_to_insert:
            # Convert row to tab-separated values
            row_values = []
            for col in columns:
                value = row.get(col, '')
                if value is None:
                    row_values.append('\\N')  # PostgreSQL NULL representation
                else:
                    # Escape special characters and convert to string
                    str_value = str(value).replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n')
                    row_values.append(str_value)
            
            buffer.write('\t'.join(row_values) + '\n')
        
        buffer.seek(0)
        
        # Use COPY command for fast insertion
        cur.copy_from(
            buffer,
            table_name,
            columns=columns,
            sep='\t',
            null='\\N'
        )
        
        return True
    except Exception as e:
        print(f"Error in COPY command: {e}")
        return False

def fast_insert_with_execute_values(cur, data_to_insert, query, page_size=10000):
    """
    Fast insertion using execute_values with optimized page size.
    """
    try:
        execute_values(cur, query, data_to_insert, page_size=page_size)
        return True
    except Exception as e:
        print(f"Error in execute_values: {e}")
        return False

def process_csv_chunk(chunk_data, variable_schema, common_identifiers, asi_survey_id, level_id):
    """
    Process a chunk of CSV data and return processed records.
    """
    processed_records = []
    
    for index, row_series in chunk_data.iterrows():
        current_record_data = {}
        unit_id_parts = []
        row_dict = row_series.to_dict()

        for var_def in variable_schema:
            var_name_in_schema = var_def['name'].upper()
            mapped_type = var_def['type']
            is_common_id = var_def['is_common_id']

            raw_value = row_dict.get(var_name_in_schema)
            if raw_value is None or pd.isna(raw_value) or str(raw_value).strip() == '':
                processed_value = None
            else:
                try:
                    if mapped_type == 'INTEGER':
                        processed_value = int(float(raw_value))
                    elif mapped_type == 'NUMERIC':
                        processed_value = float(raw_value)
                    else:
                        processed_value = str(raw_value).strip()
                except ValueError:
                    processed_value = str(raw_value).strip() if pd.notna(raw_value) else None
            
            current_record_data[var_name_in_schema] = processed_value
            if is_common_id and processed_value is not None:
                unit_id_parts.append(str(processed_value))

        if len(unit_id_parts) == len(common_identifiers) and all(p != '' for p in unit_id_parts):
            unit_identifier = "_".join(unit_id_parts)
            processed_records.append((asi_survey_id, level_id, unit_identifier, json.dumps(current_record_data)))
    
    return processed_records

def ingest_microdata_from_csv(csv_dir_path: str):
    conn = None
    cur = None
    start_time = time.time()
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Get the survey_id for ASI (assuming it's already in the 'surveys' table)
        cur.execute("SELECT survey_id FROM surveys WHERE survey_name = %s AND survey_year = %s;", ('ASI', 2023))
        result = cur.fetchone()
        if not result:
            print("Error: No survey_id found for ASI 2023 in 'surveys' table.")
            sys.exit(1)
        asi_survey_id = result[0]
        print(f"ASI 2023 Survey ID: {asi_survey_id}")

        # Load all metadata schemas from the database
        cur.execute("SELECT level_id, level_name, variable_schema, common_identifiers FROM survey_levels WHERE survey_id = %s;", (asi_survey_id,))
        all_level_metadata = {
            row[1]: {'level_id': row[0], 'variable_schema': row[2], 'common_identifiers': row[3]}
            for row in cur.fetchall()
        }
        print(f"Loaded metadata for {len(all_level_metadata)} levels from database.")

        # Check if the CSV directory exists
        if not os.path.exists(csv_dir_path):
            print(f"Critical Error: Microdata CSV directory '{csv_dir_path}' not found.")
            sys.exit(1)

        # Get all CSV files in the directory
        csv_files = [f for f in os.listdir(csv_dir_path) if f.lower().endswith('.csv')]
        if not csv_files:
            print(f"No CSV files found in '{csv_dir_path}'. Nothing to ingest.")
            sys.exit(1)

        total_inserted_records = 0
        
        for csv_filename in csv_files:
            file_start_time = time.time()
            full_csv_path = os.path.join(csv_dir_path, csv_filename)

            # Map CSV filename to the corresponding database level name
            db_level_name = CSV_FILE_TO_DB_LEVEL_NAME.get(csv_filename)
            if not db_level_name:
                db_level_name = CSV_FILE_TO_DB_LEVEL_NAME.get('default')
                print(f"Using default mapping for '{csv_filename}' -> '{db_level_name}'")

            if not db_level_name:
                print(f"Error: No database level name mapping found for CSV file '{csv_filename}'. Exiting.")
                sys.exit(1)

            if db_level_name not in all_level_metadata:
                print(f"Error: No metadata found in DB for level '{db_level_name}' (from CSV file '{csv_filename}'). Exiting.")
                sys.exit(1)

            level_info = all_level_metadata[db_level_name]
            level_id = level_info['level_id']
            variable_schema = level_info['variable_schema']
            common_identifiers = level_info['common_identifiers']

            print(f"\nProcessing CSV file: '{csv_filename}' (Mapping to DB Level: '{db_level_name}', ID: {level_id})")

            # Get the header row and delimiter for this specific CSV file
            header_row_index = CSV_HEADER_ROW_MAP.get(csv_filename, None)
            delimiter = CSV_DELIMITER_MAP.get(csv_filename, ',')

            try:
                # Load CSV with optimized settings
                if header_row_index is None:
                    df = pd.read_csv(full_csv_path, header=None, dtype=str, sep=delimiter, chunksize=BATCH_SIZE)
                    schema_headers = [var['name'] for var in variable_schema]
                    # Process first chunk to get column count
                    first_chunk = next(df)
                    if len(schema_headers) != len(first_chunk.columns):
                        print(f"Error: Number of columns in '{csv_filename}' does not match schema. Exiting.")
                        sys.exit(1)
                    first_chunk.columns = schema_headers
                    df = pd.read_csv(full_csv_path, header=None, dtype=str, sep=delimiter, chunksize=BATCH_SIZE)
                else:
                    df = pd.read_csv(full_csv_path, header=header_row_index, dtype=str, sep=delimiter, chunksize=BATCH_SIZE)
                
                # Convert all column names to uppercase for case-insensitive matching
                if header_row_index is None:
                    df = pd.read_csv(full_csv_path, header=None, dtype=str, sep=delimiter, chunksize=BATCH_SIZE)
                    for chunk in df:
                        chunk.columns = [col.upper() for col in schema_headers]
                        break
                    df = pd.read_csv(full_csv_path, header=None, dtype=str, sep=delimiter, chunksize=BATCH_SIZE)
                else:
                    df = pd.read_csv(full_csv_path, header=header_row_index, dtype=str, sep=delimiter, chunksize=BATCH_SIZE)
                
                print(f"Processing '{csv_filename}' in chunks of {BATCH_SIZE}...")
                
                chunk_count = 0
                file_total_records = 0
                file_skipped_records = 0
                file_inserted_records = 0
                
                for chunk in df:
                    chunk_count += 1
                    chunk_start_time = time.time()
                    
                    # Convert column names to uppercase
                    chunk.columns = [col.upper() for col in chunk.columns]
                    
                    print(f"  Processing chunk {chunk_count} with {len(chunk)} records...")
                    
                    # Process the chunk
                    processed_records = process_csv_chunk(
                        chunk, variable_schema, common_identifiers, asi_survey_id, level_id
                    )
                    
                    file_total_records += len(chunk)
                    file_skipped_records += (len(chunk) - len(processed_records))
                    
                    if processed_records:
                        # Choose insertion method based on configuration
                        if USE_COPY_COMMAND and len(processed_records) > 1000:
                            # Use COPY command for large chunks (fastest)
                            columns = ['survey_id', 'level_id', 'unit_identifier', 'data_payload']
                            success = fast_insert_with_copy(cur, processed_records, 'survey_data', columns)
                            if not success:
                                # Fallback to execute_values
                                query = """
                                    INSERT INTO survey_data (survey_id, level_id, unit_identifier, data_payload)
                                    VALUES %s;
                                """
                                success = fast_insert_with_execute_values(cur, query, processed_records, BATCH_SIZE)
                        else:
                            # Use execute_values for smaller chunks
                            query = """
                                INSERT INTO survey_data (survey_id, level_id, unit_identifier, data_payload)
                                VALUES %s;
                            """
                            success = fast_insert_with_execute_values(cur, query, processed_records, BATCH_SIZE)
                        
                        if success:
                            file_inserted_records += len(processed_records)
                            total_inserted_records += len(processed_records)
                            conn.commit()
                            chunk_time = time.time() - chunk_start_time
                            print(f"    ✅ Chunk {chunk_count} inserted in {chunk_time:.2f}s ({len(processed_records)} records)")
                        else:
                            print(f"    ❌ Failed to insert chunk {chunk_count}")
                            conn.rollback()
                            return
                
                file_time = time.time() - file_start_time
                print(f"✅ File '{csv_filename}' completed in {file_time:.2f}s")
                print(f"  - Total records: {file_total_records}")
                print(f"  - Skipped records: {file_skipped_records}")
                print(f"  - Inserted records: {file_inserted_records}")

            except Exception as e:
                if conn:
                    conn.rollback()
                print(f"  - Error processing CSV file '{csv_filename}': {e}")
                sys.exit(1)

        total_time = time.time() - start_time
        print(f"\n🎉 All files processed successfully!")
        print(f"📊 Total records inserted: {total_inserted_records}")
        print(f"⏱️  Total processing time: {total_time:.2f}s")
        print(f"🚀 Average speed: {total_inserted_records/total_time:.0f} records/second")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during CSV ingestion: {e}")
        if conn:
            conn.rollback()
        sys.exit(1)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            print("Database connection closed.")

# --- Main execution ---
if __name__ == "__main__":
    print("Starting optimized microdata ingestion from CSV files...")
    print(f"Configuration:")
    print(f"  - Batch size: {BATCH_SIZE}")
    print(f"  - Use COPY command: {USE_COPY_COMMAND}")
    print(f"  - Parallel processing: {USE_PARALLEL_PROCESSING}")
    print(f"  - Max workers: {MAX_WORKERS}")
    print()
    
    ingest_microdata_from_csv(MICRODATA_CSV_DIR)