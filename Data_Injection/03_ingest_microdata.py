import pandas as pd
import json
import psycopg2
from psycopg2.extras import execute_values
import os
import sys

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

def get_db_connection():
    """Establishes and returns a database connection."""
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def ingest_microdata_from_csv(csv_dir_path: str):
    conn = None
    cur = None
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

        for csv_filename in csv_files:
            full_csv_path = os.path.join(csv_dir_path, csv_filename)

            # Map CSV filename to the corresponding database level name
            # Try to find specific mapping, otherwise use default
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
            header_row_index = CSV_HEADER_ROW_MAP.get(csv_filename, None) # Default to None (no header)
            delimiter = CSV_DELIMITER_MAP.get(csv_filename, ',') # Default to comma

            try:
                if header_row_index is None:
                    df = pd.read_csv(full_csv_path, header=None, dtype=str, sep=delimiter)
                    schema_headers = [var['name'] for var in variable_schema]
                    if len(schema_headers) != len(df.columns):
                        print(f"Error: Number of columns in '{csv_filename}' does not match schema. Exiting.")
                        sys.exit(1)
                    df.columns = schema_headers
                else:
                    df = pd.read_csv(full_csv_path, header=header_row_index, dtype=str, sep=delimiter)
                # Convert all column names to uppercase for case-insensitive matching
                df.columns = [col.upper() for col in df.columns]
                print(f"Loaded DataFrame for '{csv_filename}' with shape: {df.shape} and columns: {df.columns.tolist()}")

                data_to_insert = []
                total_records = 0
                skipped_records = 0
                for index, row_series in df.iterrows():
                    print(f"Processing record {index} in '{csv_filename}'")
                    current_record_data = {}
                    unit_id_parts = []
                    row_dict = row_series.to_dict()

                    for var_def in variable_schema:
                        var_name_in_schema = var_def['name'].upper()  # Ensure schema name is uppercase
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

                    total_records += 1
                    if len(unit_id_parts) == len(common_identifiers) and all(p != '' for p in unit_id_parts):
                        unit_identifier = "_".join(unit_id_parts)
                    else:
                        print(f"Skipping record {index} in '{csv_filename}' due to incomplete Common ID. Expected: {common_identifiers}, Found parts: {unit_id_parts}")
                        skipped_records += 1
                        continue

                    data_to_insert.append((asi_survey_id, level_id, unit_identifier, json.dumps(current_record_data)))

                print(f"Finished processing all records in '{csv_filename}'. Total: {total_records}, Skipped: {skipped_records}, To insert: {len(data_to_insert)}")

                if data_to_insert:
                    query = """
                        INSERT INTO survey_data (survey_id, level_id, unit_identifier, data_payload)
                        VALUES %s;
                    """
                    execute_values(cur, query, data_to_insert, page_size=1000)
                    conn.commit()
                    print(f"  - Successfully inserted {len(data_to_insert)} records from '{csv_filename}'.")

            except Exception as e:
                if conn:
                    conn.rollback()
                print(f"  - Error processing CSV file '{csv_filename}': {e}")
                sys.exit(1)

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
    print("Starting microdata ingestion from CSV files...")
    ingest_microdata_from_csv(MICRODATA_CSV_DIR)