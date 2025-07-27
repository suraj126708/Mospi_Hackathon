import pandas as pd
import os

# --- Configuration ---
EXCEL_FILE_PATH = 'Layout_ASI 2023-24(1).xlsx' # IMPORTANT: Change this to your actual Excel file name
OUTPUT_CSV_DIR = './asi_metadata_definitions' # Directory to save the generated CSVs

# --- Optional: Specify which sheets to convert and how to name their CSVs ---
# If your Excel has sheets like 'Level1_Demographics', 'Level2_Consumption', etc.
# And you want specific output filenames.
# If this mapping is empty, the script will attempt to convert ALL sheets found.
SHEET_TO_CSV_MAPPING = {
    # 'Sheet_Name_In_Excel': 'output_filename_for_csv.csv',
    # 'Food Consumption Data': 'asi_level_food_consumption.csv',
    # 'Household Roster': 'asi_level_household_roster.csv',
    # Add all 15 mappings here if you need specific control
}

def convert_excel_to_csves(excel_file: str, output_dir: str, sheet_mapping: dict = None):
    """
    Converts each sheet in an Excel workbook into a separate CSV file.

    Args:
        excel_file: Path to the input Excel workbook (.xlsx or .xls).
        output_dir: Directory where the CSV files will be saved.
        sheet_mapping: An optional dictionary mapping Excel sheet names to desired
                       output CSV filenames. If None, all sheets are converted
                       using their sheet names.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")

    try:
        xls = pd.ExcelFile(excel_file)
        print(f"Successfully loaded Excel file: {excel_file}")

        sheets_to_process = sheet_mapping.keys() if sheet_mapping else xls.sheet_names

        for sheet_name in sheets_to_process:
            if sheet_name not in xls.sheet_names:
                print(f"Warning: Sheet '{sheet_name}' from mapping not found in Excel. Skipping.")
                continue

            output_csv_filename = sheet_mapping.get(sheet_name, f"{sheet_name.replace(' ', '_').lower()}.csv")
            output_csv_path = os.path.join(output_dir, output_csv_filename)

            try:
                # Read the entire sheet into a DataFrame
                df = xls.parse(sheet_name)

                # --- IMPORTANT: Handle potential leading zeros for IDs ---
                # If your IDs (like FSU Serial No., Sample Household No.) have leading zeros
                # that Excel might have dropped, you'll need to re-add them or ensure they are read as strings.
                # Example: df['FSU_ID'] = df['FSU_ID'].astype(str).str.zfill(5) # If FSU_ID should always be 5 digits
                # The best way to handle this is to read columns with IDs as strings initially.
                # If you know the column index/name of ID columns, specify dtype='str' in parse().
                # For simplicity, saving as is. Ensure your original Excel has these IDs formatted as text if leading zeros are critical.

                # Save the DataFrame to a CSV file
                # index=False prevents writing DataFrame index as a column in CSV
                # header=False if your original microdata CSVs (e.g., blkA202223.CSV) did not have headers.
                df.to_csv(output_csv_path, index=False, header=False)
                print(f"  - Converted sheet '{sheet_name}' to '{output_csv_filename}'")

            except Exception as e:
                print(f"  - Error converting sheet '{sheet_name}': {e}")

    except FileNotFoundError:
        print(f"Error: Excel file '{excel_file}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

# --- Main execution ---
if __name__ == "__main__":
    print(f"Starting Excel to CSV conversion for '{EXCEL_FILE_PATH}'...")
    convert_excel_to_csves(EXCEL_FILE_PATH, OUTPUT_CSV_DIR, SHEET_TO_CSV_MAPPING)
    print("\nExcel to CSV conversion process complete.")
    print(f"Your CSV files are now in the '{OUTPUT_CSV_DIR}' directory, ready for the next ingestion step.")