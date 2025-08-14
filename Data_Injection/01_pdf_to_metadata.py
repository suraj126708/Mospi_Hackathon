import pdfplumber
import json
import re
import os

def create_json_from_pdf(pdf_path, json_path):
    """
    Extracts structured data, including tables, from the ASI PDF,
    converts it to JSON, and saves it to a file using a robust
    table extraction method.

    Args:
        pdf_path (str): The file path to the input PDF.
        json_path (str): The file path for the output JSON.
    """
    if not os.path.exists(pdf_path):
        print(f"Error: The file '{pdf_path}' was not found.")
        return

    print(f"Opening '{pdf_path}'...")
    structured_data = {}
    
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

                    block_data = {
                        "description": block_name.strip(),
                        "record_identification_key": record_keys[i].strip() if i < len(record_keys) else "Not Found",
                        "fields": []
                    }

                    if not table or len(table) < 2:
                        structured_data[block_id] = block_data
                        continue

                    # The first row of the table is the header
                    # !!! THIS IS THE CORRECTED LINE !!!
                    # Added 'if h is not None else ""' to prevent error on empty header cells.
                    header = [h.replace('\n', ' ') if h is not None else "" for h in table[0]]
                    
                    # --- Process each data row in the table ---
                    for row in table[1:]:
                        cleaned_row = [str(cell).replace('\n', ' ') if cell is not None else "" for cell in row]
                        
                        if not cleaned_row or cleaned_row[0].strip().upper() == 'TOTAL':
                            break
                        
                        field_dict = {}
                        try:
                            field_dict['srl_no'] = cleaned_row[0]
                            field_dict['field_name'] = cleaned_row[1]
                            field_dict['schedule_reference'] = cleaned_row[2]
                            field_dict['description'] = cleaned_row[3]
                            field_dict['type'] = cleaned_row[4]
                            field_dict['width'] = cleaned_row[5]
                            block_data["fields"].append(field_dict)
                        except IndexError:
                            print(f"    - Warning: Skipping malformed row in {block_id}: {cleaned_row}")
                            continue
                            
                    structured_data[block_id] = block_data

        print(f"\nSaving structured data to '{json_path}'...")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(structured_data, f, indent=4)
        print("✅ Success! JSON file has been created.")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# --- Main execution block ---
if __name__ == "__main__":
    pdf_input_file = 'a.struc23 (1).pdf'
    json_output_file = 'output.json'

    create_json_from_pdf(pdf_input_file, json_output_file)