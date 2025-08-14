import os

class Config:
    """Configuration class for the Data Injection Pipeline"""
    
    # Flask Configuration
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'your-secret-key-here-change-in-production'
    DEBUG = os.environ.get('FLASK_DEBUG', 'True').lower() == 'true'
    
    # File Upload Configuration
    UPLOAD_FOLDER = 'uploads'
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max file size
    ALLOWED_EXTENSIONS = {'pdf', 'csv'}
    
    # Database Configuration (for the existing scripts)
    DB_HOST = os.environ.get('DB_HOST', 'localhost')
    DB_NAME = os.environ.get('DB_NAME', 'statathon')
    DB_USER = os.environ.get('DB_USER', 'postgres')
    DB_PASSWORD = os.environ.get('DB_PASSWORD', 'Suraj@#6708')
    
    # Pipeline Configuration
    DATA_INJECTION_DIR = os.path.join('..', 'Data_Injection')
    CSV_UPLOAD_DIR = os.path.join('..', 'Data_Injection', 'hces_microdata_csvs')
    
    # Pipeline Steps Configuration
    PIPELINE_STEPS = [
        {
            'name': 'PDF to Metadata',
            'script': '01_pdf_to_metadata.py',
            'working_dir': DATA_INJECTION_DIR,
            'progress_start': 30,
            'progress_end': 50
        },
        {
            'name': 'Metadata Ingestion',
            'script': '02_ingest_metadata.py',
            'working_dir': DATA_INJECTION_DIR,
            'progress_start': 70,
            'progress_end': 85
        },
        {
            'name': 'Microdata Ingestion',
            'script': '03_ingest_microdata.py',
            'working_dir': DATA_INJECTION_DIR,
            'progress_start': 95,
            'progress_end': 100
        }
    ]
    
    # Server Configuration
    HOST = os.environ.get('HOST', '0.0.0.0')
    PORT = int(os.environ.get('PORT', 5000))
    
    @classmethod
    def get_db_connection_string(cls):
        """Get database connection string for the existing scripts"""
        return f"host={cls.DB_HOST} dbname={cls.DB_NAME} user={cls.DB_USER} password={cls.DB_PASSWORD}"
    
    @classmethod
    def validate_config(cls):
        """Validate that all required directories exist"""
        required_dirs = [
            cls.DATA_INJECTION_DIR,
            cls.CSV_UPLOAD_DIR
        ]
        
        missing_dirs = []
        for directory in required_dirs:
            if not os.path.exists(directory):
                missing_dirs.append(directory)
        
        if missing_dirs:
            raise ValueError(f"Required directories not found: {missing_dirs}")
        
        return True
