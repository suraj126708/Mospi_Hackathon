# Data Injection Pipeline Web Interface

A modern web-based interface for orchestrating the data injection pipeline that processes PDF and CSV files through three automated steps.

## Features

- **Drag & Drop File Upload**: Easy PDF and CSV file upload with visual feedback
- **Automated Pipeline Execution**: Runs three Python scripts sequentially
- **Real-time Progress Tracking**: Live progress bar and step-by-step status updates
- **Live Logs**: Real-time pipeline execution logs
- **API Integration**: Final step for calling external APIs
- **Responsive Design**: Modern Bootstrap-based UI that works on all devices

## Pipeline Steps

1. **File Upload**: Upload PDF and CSV files
2. **PDF to Metadata**: Convert PDF structure to metadata JSON
3. **Metadata Ingestion**: Store metadata in PostgreSQL database
4. **Microdata Ingestion**: Process and store CSV data
5. **API Integration**: Call external API services

## Prerequisites

- Python 3.8 or higher
- PostgreSQL database (for the existing scripts)
- Access to the `Data_Injection` directory with the three Python scripts

## Installation

1. **Clone or navigate to the Pipeline_dataInjection directory**

   ```bash
   cd Pipeline_dataInjection
   ```

2. **Create a virtual environment (recommended)**

   ```bash
   python -m venv venv

   # On Windows
   venv\Scripts\activate

   # On macOS/Linux
   source venv/bin/activate
   ```

3. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables (optional)**
   ```bash
   # Create .env file or set environment variables
   export DB_HOST=localhost
   export DB_NAME=statathon
   export DB_USER=postgres
   export DB_PASSWORD=your_password
   export SECRET_KEY=your-secret-key
   ```

## Configuration

Edit `config.py` to customize:

- Database connection settings
- File upload limits
- Pipeline step configurations
- Server host and port

## Usage

1. **Start the application**

   ```bash
   python app.py
   ```

2. **Open your browser**
   Navigate to `http://localhost:5000`

3. **Upload files**

   - Drag and drop or click to select a PDF file
   - Drag and drop or click to select a CSV file
   - Click "Upload Files"

4. **Start the pipeline**

   - Click "Start Data Processing Pipeline"
   - Monitor progress in real-time
   - View live logs of each step

5. **API Integration**
   - After pipeline completion, enter your API URL
   - Click "Call API" to integrate with external services

## File Structure

```
Pipeline_dataInjection/
├── app.py                 # Main Flask application
├── config.py             # Configuration settings
├── requirements.txt      # Python dependencies
├── README.md            # This file
├── templates/           # HTML templates
│   └── index.html      # Main user interface
└── uploads/            # Temporary file storage (auto-created)
```

## API Endpoints

- `GET /` - Main interface
- `POST /upload` - File upload endpoint
- `POST /start_pipeline` - Start pipeline execution
- `GET /pipeline_status` - Get current pipeline status
- `POST /api_call` - Make external API calls

## Database Requirements

The pipeline requires a PostgreSQL database with the following tables (already created by your existing scripts):

- `surveys`
- `survey_levels`
- `survey_data`

## Troubleshooting

### Common Issues

1. **"Required directories not found"**

   - Ensure the `Data_Injection` directory exists relative to this folder
   - Check that all three Python scripts are present

2. **Database connection errors**

   - Verify PostgreSQL is running
   - Check database credentials in `config.py`
   - Ensure database and tables exist

3. **File upload failures**

   - Check file size limits (default: 16MB)
   - Verify file types (.pdf, .csv only)
   - Ensure uploads directory has write permissions

4. **Pipeline script errors**
   - Check that all required Python packages are installed
   - Verify the scripts can run independently
   - Check file paths and permissions

### Debug Mode

Enable debug mode for detailed error information:

```bash
export FLASK_DEBUG=True
python app.py
```

## Security Notes

- Change the default `SECRET_KEY` in production
- Restrict file upload types and sizes
- Use HTTPS in production environments
- Implement proper authentication if needed

## Development

### Adding New Pipeline Steps

1. Add the step configuration to `config.py`
2. Update the progress calculation in `app.py`
3. Add the step to the UI in `templates/index.html`

### Customizing the UI

The interface uses Bootstrap 5 and custom CSS. Modify `templates/index.html` to:

- Change colors and styling
- Add new UI elements
- Modify the step indicators
- Customize the progress display

## Support

For issues related to:

- **Pipeline scripts**: Check the original `Data_Injection` scripts
- **Web interface**: Review Flask logs and browser console
- **Database**: Check PostgreSQL logs and connection settings

## License

This project is part of the larger data injection system. Please refer to the main project license.
