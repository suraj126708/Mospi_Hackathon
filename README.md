# MoSPI Microdata API Gateway & Data Injection

This project provides a Node.js API gateway for accessing microdata from surveys, along with Python scripts for preparing and injecting survey metadata and microdata into a PostgreSQL database.

## Project Structure

```
blank/
├── Data_Injection/
│   ├── 01_prepare_metadata_from_csv.py
│   ├── 02_ingest_metadata.py
│   ├── 03_ingest_microdata.py
│   ├── annualSurvey_all_levels_structured_metadata.json
│   ├── hces_microdata_csvs/
│   │   └── blkC202223.CSV
│   ├── Layout_HCES 2023-24(1).xlsx
│   ├── prepare_all_metadata.py
│   ├── survey.sql
│   └── XlsmToJson.py
├── mospi-api-gateway/
│   ├── app.js
│   ├── config/
│   │   └── db.js
│   ├── controllers/
│   │   ├── dataController.js
│   │   └── surveyController.js
│   ├── db/
│   │   └── index.js
│   ├── package.json
│   ├── package-lock.json
│   ├── routes/
│   │   ├── data.js
│   │   └── surveys.js
│   ├── services/
│   │   ├── dataService.js
│   │   └── surveyService.js
├── texttocsv/
│   └── LEVEL - 05 ( Sec 5 & 6).txt
└── README.md
```

## Components

### 1. Data_Injection (Python)

Scripts and resources for preparing, transforming, and injecting survey metadata and microdata into the database.

- **01_prepare_metadata_from_csv.py**: Prepares metadata from CSV files.
- **02_ingest_metadata.py**: Loads metadata into the database.
- **03_ingest_microdata.py**: Loads microdata into the database.
- **prepare_all_metadata.py**: Batch metadata preparation.
- **survey.sql**: SQL schema for the survey database.
- **annualSurvey_all_levels_structured_metadata.json**: Example structured metadata.
- **hces_microdata_csvs/**: Example microdata CSVs.
- **Layout_HCES 2023-24(1).xlsx**: Survey layout reference.
- **XlsmToJson.py**: Converts Excel macro files to JSON.

### 2. mospi-api-gateway (Node.js)

Express.js API server for querying survey metadata and microdata from PostgreSQL.

- **app.js**: Main application entry point.
- **config/db.js**: Database connection configuration.
- **db/index.js**: PostgreSQL client and query helper.
- **controllers/**: Route handler logic for surveys and data.
- **routes/**: Express route definitions for API endpoints.
- **services/**: Database query logic for surveys and microdata.
- **package.json / package-lock.json**: Node.js dependencies.

### 3. texttocsv

Contains text files for conversion or reference.

## Setup Instructions

### Python (Data Injection)

1. Install dependencies (if any):
   ```bash
   pip install -r requirements.txt  # if requirements.txt exists
   ```
2. Configure your database connection in the scripts or via environment variables.
3. Run the scripts in order to prepare and inject data:
   ```bash
   python Data_Injection/01_prepare_metadata_from_csv.py
   python Data_Injection/02_ingest_metadata.py
   python Data_Injection/03_ingest_microdata.py
   ```

### Node.js (API Gateway)

1. Install dependencies:
   ```bash
   cd mospi-api-gateway
   npm install
   ```
2. Create a `.env` file in `mospi-api-gateway/` with your PostgreSQL credentials:
   ```ini
   DB_USER=your_db_user
   DB_PASSWORD=your_db_password
   DB_HOST=localhost
   DB_NAME=your_db_name
   PORT=3000
   NODE_ENV=development
   ```
3. Start the API server:
   ```bash
   node app.js
   ```

## Database Setup

- Use the `survey.sql` file in `Data_Injection/` to create the necessary tables in your PostgreSQL database.
- Make sure your database is running and accessible to both the Python scripts and the Node.js API.

## API Usage

### Endpoints

- `GET /api/v1/surveys` — List all available surveys
- `GET /api/v1/surveys/:surveyId/levels` — List levels for a specific survey
- `GET /api/v1/data/:surveyId/:levelId?page=1&limit=10&filter={"Age":{">":25}}` — Retrieve paginated, filterable microdata
- `GET /api/v1/data/:surveyId/:levelId/:unitIdentifier` — Retrieve a single microdata record by unit identifier

### Example Request

```bash
curl "http://localhost:3000/api/v1/data/1/2?page=1&limit=5&filter={\"Age\":{\">\":25}}"
```

## Troubleshooting

- **Database connection errors**: Ensure your `.env` file has the correct credentials and your database is running.
- **Line ending warnings**: These are normal on Windows. See `.gitattributes` for configuration.
- **Submodule errors**: If you see errors about submodules, check for a `.gitmodules` file or `.git` directories inside subfolders.

## License

MIT or as specified by project owner.
