CREATE TABLE survey_data (
    data_id BIGSERIAL PRIMARY KEY,    -- Auto-incrementing unique ID for each data record
    survey_id INTEGER NOT NULL,       -- Foreign Key linking to the 'surveys' table (e.g., HCES 2024)
    level_id INTEGER NOT NULL,        -- Foreign Key linking to the 'survey_levels' table (e.g., Level 01)
    
    -- A concatenated string of common identifier components for easy lookup across levels.
    -- This 'unit_identifier' allows you to link records from different sections (e.g., household demographics
    -- from one section with food consumption from another) that belong to the same logical unit.
    -- Example: "46667_01_F_05" (FSU_No_Household_No_Ques_Type_Visit_No)
    unit_identifier TEXT NOT NULL,    
    
    -- JSONB column to store all the processed data for this specific record.
    -- This is the key to handling "different data" for each of your 15 sections.
    -- The structure of this JSON object will conform to the 'variable_schema' of its 'level_id'.
    -- Example content for a food consumption section record:
    -- {
    --   "FSU Serial No.": 46667,
    --   "Sample hhld. No.": 1,
    --   "Item Code": "RICE",
    --   "Consumption Value": 250.50,
    --   "Unit": "KG"
    -- }
    -- Example content for a demographic section record:
    -- {
    --   "FSU Serial No.": 46667,
    --   "Sample hhld. No.": 1,
    --   "Household Size": 5,
    --   "Head of Household Gender": "Male"
    -- }
    data_payload JSONB NOT NULL,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraints
    CONSTRAINT fk_survey_data_survey_id FOREIGN KEY (survey_id) REFERENCES surveys(survey_id) ON DELETE CASCADE,
    CONSTRAINT fk_survey_data_level_id FOREIGN KEY (level_id) REFERENCES survey_levels(level_id) ON DELETE CASCADE
);

-- Recommended Indexes for Performance
CREATE INDEX idx_survey_data_unit_identifier ON survey_data (unit_identifier);
CREATE INDEX idx_survey_data_survey_id ON survey_data (survey_id);
CREATE INDEX idx_survey_data_level_id ON survey_data (level_id);
-- Optional: CREATE INDEX idx_survey_data_data_payload_gin ON survey_data USING GIN (data_payload);

