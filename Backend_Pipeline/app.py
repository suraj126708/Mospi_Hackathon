from flask import Flask, render_template, request, jsonify, session, redirect, url_for
import os
import subprocess
import json
import threading
import time
from werkzeug.utils import secure_filename
import shutil

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'  # Change this in production

# Configuration
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'pdf', 'csv'}
MAX_CONTENT_LENGTH = 100 * 1024 * 1024  # 100MB max file size

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH

# Global variables for pipeline status
pipeline_status = {
    'running': False,
    'current_step': '',
    'progress': 0,
    'logs': [],
    'completed': False,
    'error': None
}

def allowed_file(filename, allowed_extensions):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in allowed_extensions

def ensure_upload_folder():
    """Ensure upload folder exists"""
    if not os.path.exists(UPLOAD_FOLDER):
        os.makedirs(UPLOAD_FOLDER)

def run_pipeline_step(step_name, command, working_dir):
    """Run a pipeline step and return success status"""
    try:
        pipeline_status['current_step'] = step_name
        pipeline_status['logs'].append(f"Starting {step_name}...")
        
        # Run the command
        result = subprocess.run(
            command,
            cwd=working_dir,
            capture_output=True,
            text=True,
            shell=True
        )
        
        # Log both stdout and stderr for debugging
        if result.stdout:
            pipeline_status['logs'].append(f"📋 {step_name} output: {result.stdout.strip()}")
        if result.stderr:
            pipeline_status['logs'].append(f"⚠️ {step_name} stderr: {result.stderr.strip()}")
        
        if result.returncode == 0:
            pipeline_status['logs'].append(f"✅ {step_name} completed successfully")
            return True
        else:
            error_msg = f"❌ {step_name} failed with return code {result.returncode}"
            if result.stderr:
                error_msg += f": {result.stderr.strip()}"
            elif result.stdout:
                error_msg += f": {result.stdout.strip()}"
            pipeline_status['logs'].append(error_msg)
            return False
            
    except Exception as e:
        pipeline_status['logs'].append(f"❌ Error in {step_name}: {str(e)}")
        return False

def run_pipeline(pdf_filename, csv_filename):
    """Run the complete pipeline"""
    global pipeline_status
    
    try:
        pipeline_status['running'] = True
        pipeline_status['progress'] = 0
        pipeline_status['logs'] = []
        pipeline_status['error'] = None
        pipeline_status['completed'] = False
        
        # Use the passed filenames instead of session
        if not pdf_filename or not csv_filename:
            pipeline_status['error'] = "No files provided"
            return
        
        # Step 1: Copy files to Data_Injection directory
        pipeline_status['current_step'] = "Preparing files..."
        pipeline_status['progress'] = 10
        
        source_pdf = os.path.join(UPLOAD_FOLDER, pdf_filename)
        source_csv = os.path.join(UPLOAD_FOLDER, csv_filename)
        
        # Copy PDF to Data_Injection directory
        dest_pdf = os.path.join('..', 'Data_Injection', pdf_filename)
        shutil.copy2(source_pdf, dest_pdf)
        
        # Copy CSV to hces_microdata_csvs directory
        csv_dir = os.path.join('..', 'Data_Injection', 'hces_microdata_csvs')
        if not os.path.exists(csv_dir):
            os.makedirs(csv_dir)
        dest_csv = os.path.join(csv_dir, csv_filename)
        shutil.copy2(source_csv, dest_csv)
        
        pipeline_status['progress'] = 20
        pipeline_status['logs'].append("Files copied to Data_Injection directory")
        
        # Step 2: Run PDF to metadata conversion
        pipeline_status['progress'] = 30
        if not run_pipeline_step("PDF to Metadata", "python 01_pdf_to_metadata.py", "../Data_Injection"):
            pipeline_status['error'] = "PDF to metadata conversion failed"
            return
        
        pipeline_status['progress'] = 50
        
        # Step 3: Ingest metadata
        pipeline_status['progress'] = 70
        if not run_pipeline_step("Metadata Ingestion", "python 02_ingest_metadata.py", "../Data_Injection"):
            pipeline_status['error'] = "Metadata ingestion failed"
            return
        
        pipeline_status['progress'] = 85
        
        # Step 4: Ingest microdata (Ultra-Fast)
        pipeline_status['progress'] = 95
        if not run_pipeline_step("Ultra-Fast Microdata Ingestion", "python ultra_fast_microdata.py", "."):
            pipeline_status['error'] = "Ultra-fast microdata ingestion failed"
            return
        
        pipeline_status['progress'] = 100
        pipeline_status['completed'] = True
        pipeline_status['logs'].append("🎉 Pipeline completed successfully!")
        
    except Exception as e:
        pipeline_status['error'] = f"Pipeline error: {str(e)}"
        pipeline_status['logs'].append(f"❌ Critical error: {str(e)}")
    finally:
        pipeline_status['running'] = False

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/test')
def test():
    """Test endpoint to verify Flask is working"""
    return jsonify({'status': 'ok', 'message': 'Flask app is running'})

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': time.time(),
        'pipeline_running': pipeline_status['running']
    })

@app.route('/test-microdata')
def test_microdata():
    """Test the ultra-fast microdata ingestion script directly"""
    try:
        result = subprocess.run(
            "python ultra_fast_microdata.py",
            cwd=".",
            capture_output=True,
            text=True,
            shell=True
        )
        
        return jsonify({
            'return_code': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'success': result.returncode == 0
        })
    except Exception as e:
        return jsonify({
            'error': str(e),
            'success': False
        })

@app.route('/test-db')
def test_database():
    """Test database connection and required tables"""
    try:
        import psycopg2
        
        # Test connection
        conn = psycopg2.connect(
            host="localhost",
            database="statathon",
            user="postgres",
            password="Suraj@#6708"
        )
        
        cur = conn.cursor()
        
        # Check if required tables exist
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('surveys', 'survey_levels', 'survey_data')
        """)
        
        existing_tables = [row[0] for row in cur.fetchall()]
        
        # Check if ASI survey exists
        cur.execute("SELECT survey_id, survey_name, survey_year FROM surveys WHERE survey_name = 'ASI' AND survey_year = 2023")
        asi_survey = cur.fetchone()
        
        # Check if ASI_BLOCK_C level exists
        cur.execute("SELECT level_id, level_name FROM survey_levels WHERE level_name = 'ASI_BLOCK_C'")
        asi_block_c = cur.fetchone()
        
        cur.close()
        conn.close()
        
        return jsonify({
            'connection': 'success',
            'existing_tables': existing_tables,
            'asi_survey': asi_survey,
            'asi_block_c_level': asi_block_c,
            'missing_tables': [t for t in ['surveys', 'survey_levels', 'survey_data'] if t not in existing_tables]
        })
        
    except Exception as e:
        return jsonify({
            'connection': 'failed',
            'error': str(e)
        })

@app.route('/upload', methods=['POST'])
def upload_files():
    """Handle file uploads"""
    try:
        print(f"Upload request received. Files: {list(request.files.keys())}")
        
        # Check if files were uploaded
        if 'pdf_file' not in request.files or 'csv_file' not in request.files:
            print(f"Missing files. Found: {list(request.files.keys())}")
            return jsonify({'error': 'Both PDF and CSV files are required'}), 400
        
        pdf_file = request.files['pdf_file']
        csv_file = request.files['csv_file']
        
        print(f"PDF file: {pdf_file.filename}, CSV file: {csv_file.filename}")
        
        # Check if files are selected
        if pdf_file.filename == '' or csv_file.filename == '':
            print("Empty filenames detected")
            return jsonify({'error': 'Both files must be selected'}), 400
        
        # Validate file types
        if not allowed_file(pdf_file.filename, {'pdf'}):
            print(f"Invalid PDF file type: {pdf_file.filename}")
            return jsonify({'error': 'First file must be a PDF'}), 400
        
        if not allowed_file(csv_file.filename, {'csv'}):
            print(f"Invalid CSV file type: {csv_file.filename}")
            return jsonify({'error': 'Second file must be a CSV'}), 400
        
        # Ensure upload folder exists
        ensure_upload_folder()
        print(f"Upload folder ensured: {UPLOAD_FOLDER}")
        
        # Save files
        pdf_filename = secure_filename(pdf_file.filename)
        csv_filename = secure_filename(csv_file.filename)
        
        pdf_path = os.path.join(UPLOAD_FOLDER, pdf_filename)
        csv_path = os.path.join(UPLOAD_FOLDER, csv_filename)
        
        print(f"Saving PDF to: {pdf_path}")
        pdf_file.save(pdf_path)
        print(f"Saving CSV to: {csv_path}")
        csv_file.save(csv_path)
        
        # Store filenames in session
        session['pdf_file'] = pdf_filename
        session['csv_file'] = csv_filename
        
        print(f"Files uploaded successfully: {pdf_filename}, {csv_filename}")
        return jsonify({
            'success': True,
            'message': 'Files uploaded successfully',
            'pdf_file': pdf_filename,
            'csv_file': csv_filename
        })
        
    except Exception as e:
        print(f"Error in upload_files: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Upload failed: {str(e)}'}), 500

@app.route('/start_pipeline', methods=['POST'])
def start_pipeline():
    """Start the pipeline execution"""
    if pipeline_status['running']:
        return jsonify({'error': 'Pipeline is already running'}), 400
    
    # Get filenames from session
    pdf_filename = session.get('pdf_file')
    csv_filename = session.get('csv_file')
    
    if not pdf_filename or not csv_filename:
        return jsonify({'error': 'No files found. Please upload files first.'}), 400
    
    # Start pipeline in background thread with filenames
    thread = threading.Thread(target=run_pipeline, args=(pdf_filename, csv_filename))
    thread.daemon = True
    thread.start()
    
    return jsonify({'success': True, 'message': 'Pipeline started'})

@app.route('/pipeline_status')
def get_pipeline_status():
    """Get current pipeline status"""
    return jsonify(pipeline_status)

@app.route('/api_call', methods=['POST'])
def make_api_call():
    """Handle the final API call"""
    try:
        api_url = request.json.get('api_url')
        if not api_url:
            return jsonify({'error': 'API URL is required'}), 400
        
        # Here you would make the call to your existing API
        # For now, just return success
        return jsonify({
            'success': True,
            'message': f'API call to {api_url} would be made here',
            'api_url': api_url
        })
        
    except Exception as e:
        return jsonify({'error': f'API call failed: {str(e)}'}), 500

if __name__ == '__main__':
    ensure_upload_folder()
    app.run(debug=True, host='0.0.0.0', port=5000)
