import pandas as pd
import json

# 1. Load the layout sheet into a DataFrame
df = pd.read_excel(
    "Layout_HCES 2023-24.xlsx",
    sheet_name="Sheet2",
    header=None,
    dtype=str
)

# 2. Identify the rows where a new level begins by finding "LEVEL - XX"
level_rows = (
    df[1]
    .fillna("")
    .str.contains(r"LEVEL\s*-\s*\d{2}")
)
level_indices = df[level_rows].index.tolist() + [len(df)]

levels = []

# 3. Iterate each level block
for i in range(len(level_indices) - 1):
    start = level_indices[i]
    end = level_indices[i+1]
    header = df.at[start, 1]
    # Extract the level number for naming
    lvl_code = header.split("LEVEL")[-1].split("(")[0].strip().replace("-", "_").replace(" ", "")
    level_name = f"HCES_LEVEL_{lvl_code}"

    # 4. The variable rows begin two rows below the header
    block = df.iloc[start+2:end].dropna(axis=1, how="all").reset_index(drop=True)
    vschema = []
    common_ids = []

    # 5. Parse each non-empty row of the block
    for _, row in block.iterrows():
        # stop at fully blank or if Sl.No. missing
        if pd.isna(row[0]):
            break
        item = {
            "name": row[1].strip(),
            "description": (row[9] or "").strip(),
            "length": int(row[6]),
            "start_position": int(row[7]),
            "end_position": int(row[8]),
            "type": "TEXT" if row[6] in ("4","38") and "Generated" in (row[9] or "") else
                    ("NUMERIC" if int(row[6])>4 else "INTEGER"),
            "is_common_id": "Common-ID" in (row[1] or "") or "**Common-ID**" in (row[9] or "") or row[3] in ("1.12", "1.11", "All")
        }
        vschema.append(item)
        if item["is_common_id"]:
            common_ids.append(item["name"])

    levels.append({
        "level_name": level_name,
        "variable_schema": vschema,
        "common_identifiers": common_ids
    })

# 6. Dump to JSON
with open("hces_schema_all_levels.json", "w", encoding="utf-8") as f:
    json.dump(levels, f, indent=2, ensure_ascii=False)

print("JSON schema for all levels written to hces_schema_all_levels.json")
