#!/usr/bin/env python3
"""
Startup script for the Data Injection Pipeline Web Interface
"""

import os
import sys
import logging
from pathlib import Path

# Add the current directory to Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

try:
    from app import app
    from config import Config
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    def main():
        """Main startup function"""
        try:
            # Validate configuration
            logger.info("Validating configuration...")
            Config.validate_config()
            logger.info("✅ Configuration validation passed")
            
            # Ensure upload directory exists
            upload_dir = Path(Config.UPLOAD_FOLDER)
            upload_dir.mkdir(exist_ok=True)
            logger.info(f"✅ Upload directory ready: {upload_dir.absolute()}")
            
            # Check if Data_Injection directory exists
            data_injection_dir = Path(Config.DATA_INJECTION_DIR)
            if not data_injection_dir.exists():
                logger.error(f"❌ Data_Injection directory not found: {data_injection_dir.absolute()}")
                logger.error("Please ensure the Data_Injection directory exists relative to this folder")
                sys.exit(1)
            
            # Check if required Python scripts exist
            required_scripts = [
                '01_pdf_to_metadata.py',
                '02_ingest_metadata.py', 
                '03_ingest_microdata.py'
            ]
            
            missing_scripts = []
            for script in required_scripts:
                script_path = data_injection_dir / script
                if not script_path.exists():
                    missing_scripts.append(script)
            
            if missing_scripts:
                logger.error(f"❌ Missing required scripts: {missing_scripts}")
                logger.error(f"Please ensure all scripts exist in: {data_injection_dir.absolute()}")
                sys.exit(1)
            
            logger.info("✅ All required scripts found")
            
            # Display startup information
            print("\n" + "="*60)
            print("🚀 Data Injection Pipeline Web Interface")
            print("="*60)
            print(f"📁 Data Injection Directory: {data_injection_dir.absolute()}")
            print(f"📁 Upload Directory: {upload_dir.absolute()}")
            print(f"🌐 Server URL: http://{Config.HOST}:{Config.PORT}")
            print(f"🔧 Debug Mode: {Config.DEBUG}")
            print("="*60)
            print("Starting Flask application...\n")
            
            # Start the Flask application
            app.run(
                host=Config.HOST,
                port=Config.PORT,
                debug=Config.DEBUG
            )
            
        except ValueError as e:
            logger.error(f"❌ Configuration error: {e}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
            sys.exit(1)
    
    if __name__ == "__main__":
        main()
        
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Please ensure all dependencies are installed:")
    print("  pip install -r requirements.txt")
    sys.exit(1)
except Exception as e:
    print(f"❌ Startup error: {e}")
    sys.exit(1)
