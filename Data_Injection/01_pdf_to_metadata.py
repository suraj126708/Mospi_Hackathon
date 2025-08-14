import pdfplumber
import json
import re
import os

def create_structured_metadata_from_pdf(pdf_path, json_path):
    """
    Extracts structured data, including tables, from the ASI PDF,
    converts it to structured metadata JSON format with calculated positions
    and standardized field types.

    Args:
        pdf_path (str): The file path to the input PDF.
        json_path (str): The file path for the output JSON.
    """
    if not os.path.exists(pdf_path):
        print(f"Error: The file '{pdf_path}' was not found.")
        return

    print(f"Opening '{pdf_path}'...")
    structured_metadata = []
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            # --- Iterate through each page of the PDF ---
            for page_num, page in enumerate(pdf.pages):
                print(f"Processing Page {page_num + 1}...")
                
                page_text = page.extract_text()
                tables = page.extract_tables()

                # Find all block titles on the page, e.g., "BLOCK-A (IDENTIFICATION...)"
                block_titles = re.findall(r"BLOCK-([A-Z])\s*\((.*?)\)", page_text)
                
                # Find all record keys on the page
                record_keys = re.findall(r"Record Identification Key for Block-[A-Z]:\s*(.*)", page_text)

                if len(block_titles) != len(tables):
                    print(f"Warning: Mismatch between found blocks ({len(block_titles)}) and tables ({len(tables)}) on page {page_num + 1}. Skipping page.")
                    continue
                
                for i, (block_letter, block_name) in enumerate(block_titles):
                    block_id = f"BLOCK-{block_letter}"
                    table = tables[i]
                    
                    print(f"  -> Found {block_id}: {block_name.strip()}")

                    # Create level name
                    level_name = f"ASI_BLOCK_{block_letter}"
                    
                    # Initialize variables for position calculation
                    current_position = 1
                    variable_schema = []

                    if not table or len(table) < 2:
                        # Create empty block metadata if no table
                        block_metadata = {
                            "level_name": level_name,
                            "variable_schema": variable_schema,
                            "common_identifiers": []
                        }
                        structured_metadata.append(block_metadata)
                        continue

                    # The first row of the table is the header
                    # Added 'if h is not None else ""' to prevent error on empty header cells.
                    header = [h.replace('\n', ' ') if h is not None else "" for h in table[0]]
                    
                    # --- Process each data row in the table ---
                    for row in table[1:]:
                        cleaned_row = [str(cell).replace('\n', ' ') if cell is not None else "" for cell in row]
                        
                        if not cleaned_row or cleaned_row[0].strip().upper() == 'TOTAL':
                            break
                        
                        # Skip header rows and empty field names
                        if (cleaned_row[0] in ['BLOCK-A (IDENTIFICATION BLOCK FOR OFFICIAL USE)', 'Srl.No.', ''] or 
                            not cleaned_row[1].strip()):
                            continue
                        
                        try:
                            # Get field width (length)
                            field_width = 0
                            if len(cleaned_row) > 5 and cleaned_row[5].strip():
                                try:
                                    field_width = int(cleaned_row[5])
                                except ValueError:
                                    field_width = 0
                            
                            # Determine field type
                            field_type = determine_field_type(cleaned_row[4], cleaned_row[3])
                            
                            # Check if this is a common identifier
                            is_common_id = cleaned_row[1] == 'BLK'
                            
                            # Create variable schema entry
                            variable_entry = {
                                "name": cleaned_row[1],
                                "description": cleaned_row[3],
                                "length": field_width,
                                "start_position": current_position,
                                "end_position": current_position + field_width - 1 if field_width > 0 else current_position,
                                "type": field_type,
                                "is_common_id": is_common_id
                            }
                            
                            variable_schema.append(variable_entry)
                            
                            # Update position for next field
                            if field_width > 0:
                                current_position += field_width
                            else:
                                # If width is not specified, use a default of 1
                                current_position += 1
                                
                        except IndexError:
                            print(f"    - Warning: Skipping malformed row in {block_id}: {cleaned_row}")
                            continue
                    
                    # Identify common identifiers
                    common_identifiers = [var["name"] for var in variable_schema if var["is_common_id"]]
                    
                    # Create block metadata
                    block_metadata = {
                        "level_name": level_name,
                        "variable_schema": variable_schema,
                        "common_identifiers": common_identifiers
                    }
                    
                    structured_metadata.append(block_metadata)

        print(f"\nSaving structured metadata to '{json_path}'...")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(structured_metadata, f, indent=2)
        print("✅ Success! Structured metadata JSON file has been created.")
        
        # Print summary
        print(f"\n📊 Summary:")
        print(f"Total blocks processed: {len(structured_metadata)}")
        for block in structured_metadata:
            print(f"  - {block['level_name']}: {len(block['variable_schema'])} fields")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def determine_field_type(type_str, description):
    """
    Determines the standardized field type based on the original type and description.
    
    Args:
        type_str (str): Original type from the PDF
        description (str): Field description
    
    Returns:
        str: Standardized field type (TEXT, INTEGER, NUMERIC)
    """
    if not type_str:
        # Try to infer from description
        if any(word in description.lower() for word in ['code', 'id', 'no', 'number']):
            return "INTEGER"
        elif any(word in description.lower() for word in ['rs', 'rupees', 'value', 'amount', 'cost']):
            return "NUMERIC"
        else:
            return "TEXT"
    
    type_lower = type_str.lower()
    
    if 'character' in type_lower or 'text' in type_lower:
        return "TEXT"
    elif 'numeric' in type_lower or 'integer' in type_lower:
        return "INTEGER"
    elif 'decimal' in type_lower or 'float' in type_lower:
        return "NUMERIC"
    else:
        # Default inference based on description
        if any(word in description.lower() for word in ['code', 'id', 'no', 'number']):
            return "INTEGER"
        elif any(word in description.lower() for word in ['rs', 'rupees', 'value', 'amount', 'cost']):
            return "NUMERIC"
        else:
            return "TEXT"

# --- Main execution block ---
if __name__ == "__main__":
    pdf_input_file = 'a.struc23 (1).pdf'
    json_output_file = 'annualSurvey_all_levels_structured_metadata.json'

    create_structured_metadata_from_pdf(pdf_input_file, json_output_file)