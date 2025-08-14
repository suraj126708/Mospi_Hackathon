import pandas as pd
import json
import re
import os

# --- Configuration ---
METADATA_CSV_FILE = './asi_metadata_definitions/sheet2.csv'
OUTPUT_JSON_FILE = 'annualSurvey_all_levels_structured_metadata.json'

def parse_metadata_csv(csv_file_path: str) -> list:
    """
    Parses the multi-level CSV metadata file with different headers for each section.
    """
    all_levels_metadata = []
    
    try:
        # Read the entire CSV file as text to identify sections
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        current_section = None
        current_level_data = []
        current_headers = None
        
        for i, line in enumerate(lines):
            line = line.strip()
            
            # Check if this is a new section header (contains "LEVEL" and file name)
            if "LEVEL" in line and "File Name:" in line:
                # Process previous section if exists
                if current_section and current_level_data:
                    level_metadata = process_section(current_section, current_level_data, current_headers)
                    if level_metadata:
                        all_levels_metadata.append(level_metadata)
                
                # Start new section
                current_section = line
                current_level_data = []
                current_headers = None
                print(f"Found new section: {line}")
                
            # Check if this is a header row (contains column names)
            elif any(keyword in line for keyword in ['Sl.No.', 'srl. no.', 'Item', 'Length', 'Byte Position']):
                current_headers = line
                print(f"Found headers: {line}")
                
            # Skip empty lines and notes
            elif line and not line.startswith(',') and not "NOTE" in line:
                current_level_data.append(line)
        
        # Process the last section
        if current_section and current_level_data:
            level_metadata = process_section(current_section, current_level_data, current_headers)
            if level_metadata:
                all_levels_metadata.append(level_metadata)
                
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return []
    
    return all_levels_metadata

def process_section(section_header: str, data_lines: list, headers: str) -> dict:
    """
    Process a single section of the CSV file.
    """
    # Extract level information from section header
    level_match = re.search(r'LEVEL\s*-\s*(\d+)', section_header)
    level_num = level_match.group(1) if level_match else "UNKNOWN"
    
    # Extract questionnaire type
    questionnaire_match = re.search(r'Ques\.\s*(\w+)', section_header)
    questionnaire_type = questionnaire_match.group(1) if questionnaire_match else "UNKNOWN"
    
    # Extract file name
    file_match = re.search(r'File Name:\s*([^,\s]+)', section_header)
    file_name = file_match.group(1) if file_match else f"asi23_lvl_{level_num.zfill(2)}.txt"
    
    level_name = f"LEVEL_{level_num}_{questionnaire_type}"
    
    print(f"Processing {level_name} with {len(data_lines)} data lines")
    
    # Parse the data lines based on the header structure
    variable_schema = []
    common_identifiers = []
    
    for line in data_lines:
        if not line.strip():
            continue
            
        # Split by comma, but handle quoted fields
        fields = []
        current_field = ""
        in_quotes = False
        
        for char in line:
            if char == '"':
                in_quotes = not in_quotes
            elif char == ',' and not in_quotes:
                fields.append(current_field.strip())
                current_field = ""
            else:
                current_field += char
        
        fields.append(current_field.strip())  # Add the last field
        
        # Skip if not enough fields
        if len(fields) < 6:
            continue
        
        # Extract variable information based on the structure
        try:
            # Different sections have different column structures
            if "srl. no." in headers.lower():
                # Level 01 structure
                var_name = fields[1] if len(fields) > 1 else ""
                length_str = fields[5] if len(fields) > 5 else ""
                byte_pos_str = fields[6] if len(fields) > 6 else ""
                remarks = fields[9] if len(fields) > 9 else ""
            else:
                # Other levels structure
                var_name = fields[1] if len(fields) > 1 else ""
                length_str = fields[5] if len(fields) > 5 else ""
                byte_pos_str = fields[6] if len(fields) > 6 else ""
                remarks = fields[9] if len(fields) > 9 else ""
            
            # Skip if essential fields are missing
            if not var_name or not length_str or not byte_pos_str:
                continue
            
            # Parse length
            try:
                length = int(float(length_str))
            except ValueError:
                continue
            
            # Parse byte positions
            byte_pos_match = re.search(r'(\d+)\s*,\s*-\s*,\s*(\d+\.?\d*)', byte_pos_str)
            if byte_pos_match:
                start_pos = int(byte_pos_match.group(1))
                end_pos = int(float(byte_pos_match.group(2)))
            else:
                start_pos = None
                end_pos = None
            
            # Determine if it's a common ID
            is_common_id = "**Common-ID**" in remarks or "Common-ID" in var_name
            
            # Infer data type
            inferred_type = infer_data_type(var_name, length, remarks)
            
            variable_info = {
                "name": var_name,
                "description": remarks,
                "length": length,
                "start_position": start_pos,
                "end_position": end_pos,
                "type": inferred_type,
                "is_common_id": is_common_id
            }
            
            variable_schema.append(variable_info)
            
            if is_common_id:
                common_identifiers.append(var_name)
                
        except Exception as e:
            print(f"Error processing line: {line[:50]}... - {e}")
            continue
    
    # Create the level metadata
    level_metadata = {
        "level_name": level_name,
        "level_number": level_num,
        "questionnaire_type": questionnaire_type,
        "file_name": file_name,
        "variable_schema": variable_schema,
        "common_identifiers": list(set(common_identifiers))
    }
    
    print(f"Processed {len(variable_schema)} variables for {level_name}")
    return level_metadata

def infer_data_type(var_name: str, length: int, remarks: str) -> str:
    """
    Infer the data type based on variable name, length, and remarks.
    """
    lower_var_name = var_name.lower()
    lower_remarks = remarks.lower()
    
    if any(keyword in lower_var_name for keyword in ["code", "no.", "serial", "id"]):
        return 'INTEGER'
    if "multiplier" in lower_var_name:
        return 'NUMERIC'
    if "value" in lower_var_name or "amount" in lower_var_name or "rs." in lower_remarks:
        return 'NUMERIC'
    if length <= 4:
        return 'INTEGER'
    elif length > 4:
        return 'TEXT'
    
    return 'TEXT'

# --- Main execution ---
if __name__ == "__main__":
    print(f"Attempting to parse metadata from: {METADATA_CSV_FILE}")
    
    # Check if the file exists
    if not os.path.exists(METADATA_CSV_FILE):
        print(f"Error: The file '{METADATA_CSV_FILE}' was not found.")
        print("Please ensure your CSV file is in the correct location.")
    else:
        structured_metadata = parse_metadata_csv(METADATA_CSV_FILE)
        
        if structured_metadata:
            try:
                with open(OUTPUT_JSON_FILE, 'w', encoding='utf-8') as f:
                    json.dump(structured_metadata, f, indent=4, ensure_ascii=False)
                print(f"Successfully saved structured metadata to '{OUTPUT_JSON_FILE}'.")
                print(f"Metadata for {len(structured_metadata)} survey levels generated.")
                print(f"This file is now ready to be used by your `02_ingest_metadata.py` script.")
                
                # Print summary
                for level in structured_metadata:
                    print(f"- {level['level_name']}: {len(level['variable_schema'])} variables")
                    
            except Exception as e:
                print(f"Error saving JSON file: {e}")
        else:
            print("No structured metadata was generated due to previous errors.")