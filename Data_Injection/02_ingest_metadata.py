import psycopg2
import json
import os

# --- Database Connection Configuration ---
DB_HOST = "localhost"
DB_NAME = "statathon" # Ensure this matches your PostgreSQL database name
DB_USER = "postgres"  # Your PostgreSQL username
DB_PASSWORD = "Suraj@#6708" 

STRUCTURED_METADATA_JSON_FILE = 'annualSurvey_all_levels_structured_metadata.json'

def get_db_connection():
    """Establishes and returns a database connection."""
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    return conn

def ingest_metadata():
    """
    Reads structured metadata from a JSON file and inserts it into
    the 'surveys' and 'survey_levels' tables in PostgreSQL.
    """
    if not os.path.exists(STRUCTURED_METADATA_JSON_FILE):
        print(f"Error: Structured metadata JSON file not found at '{STRUCTURED_METADATA_JSON_FILE}'.")
        print("Please ensure you have run '01_prepare_metadata_from_csv.py' successfully.")
        return

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        print(f"Reading structured metadata from: {STRUCTURED_METADATA_JSON_FILE}")
        with open(STRUCTURED_METADATA_JSON_FILE, 'r', encoding='utf-8') as f:
            all_levels_metadata = json.load(f)

        if not all_levels_metadata:
            print("No structured metadata found in the JSON file. Nothing to ingest.")
            return

        # --- 1. Ingest into 'surveys' table ---
        # Assuming ASI 2023 is the survey.
        # This will either insert a new survey or do nothing if it already exists (ON CONFLICT).
        survey_name = "ASI"
        survey_year = 2023
        survey_description = "Annual Survey of Industries"

        print(f"Ensuring '{survey_name} {survey_year}' survey entry exists...")
        cur.execute("""
            INSERT INTO surveys (survey_name, survey_year, description)
            VALUES (%s, %s, %s)
            ON CONFLICT (survey_name, survey_year) DO NOTHING;
        """, (survey_name, survey_year, survey_description))
        conn.commit()

        # Get the survey_id for ASI 2023
        cur.execute("SELECT survey_id FROM surveys WHERE survey_name = %s AND survey_year = %s;", (survey_name, survey_year))
        asi_survey_id = cur.fetchone()[0]
        print(f"ASI {survey_year} Survey ID: {asi_survey_id}")

        # --- 2. Ingest into 'survey_levels' table ---
        print(f"Ingesting metadata for {len(all_levels_metadata)} level(s) into 'survey_levels' table...")
        for level_data in all_levels_metadata:
            level_name = level_data['level_name']
            variable_schema = json.dumps(level_data['variable_schema']) # Store JSONB
            common_identifiers = json.dumps(level_data['common_identifiers']) # Store JSONB

            try:
                cur.execute("""
                    INSERT INTO survey_levels (survey_id, level_name, variable_schema, common_identifiers)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (survey_id, level_name) DO NOTHING
                    RETURNING level_id;
                """, (asi_survey_id, level_name, variable_schema, common_identifiers))
                
                level_id = cur.fetchone()[0] if cur.rowcount > 0 else None
                if level_id:
                    print(f" - Metadata for '{level_name}' inserted with Level ID: {level_id}")
                else:
                    cur.execute("SELECT level_id FROM survey_levels WHERE survey_id = %s AND level_name = %s;", (asi_survey_id, level_name))
                    level_id = cur.fetchone()[0]
                    print(f" - Metadata for '{level_name}' already exists with Level ID: {level_id}")
                
                conn.commit() # Commit each level's metadata or commit after all
            except Exception as e:
                print(f" - Error inserting metadata for level '{level_name}': {e}")
                conn.rollback() # Rollback if an error occurs for a specific level

        print("\nMetadata ingestion process completed.")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback() # Rollback any pending transactions
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()
            print("Database connection closed.")

# --- Main execution ---
if __name__ == "__main__":
    ingest_metadata()