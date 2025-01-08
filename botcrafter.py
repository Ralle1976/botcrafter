import os
import logging
from typing import Dict, Any, Optional, List
from functools import wraps
import mysql.connector
from mysql.connector import pooling
from flask import Flask, request, jsonify, Response
from dotenv import load_dotenv

# Logging-Konfiguration mit formatierter Ausgabe
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Umgebungsvariablen laden
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# Flask-App erstellen
app = Flask(__name__)

# Konfigurationskonstanten
API_TOKEN = os.getenv("API_TOKEN")
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "pool_name": "mypool",
    "pool_size": 5
}

# Connection Pool erstellen
try:
    connection_pool = mysql.connector.pooling.MySQLConnectionPool(**DB_CONFIG)
    logger.info("Database connection pool initialized successfully")
except mysql.connector.Error as e:
    logger.error(f"Error creating connection pool: {e}")
    raise

def require_auth(f):
    """Decorator für API-Token-Authentifizierung"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get("Authorization")
        if token != API_TOKEN:
            return jsonify({"status": "error", "message": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated_function

class DatabaseManager:
    """Klasse für das Datenbankmanagement"""
    @staticmethod
    def get_connection():
        """Verbindung aus dem Pool holen"""
        return connection_pool.get_connection()

    @staticmethod
    def execute_query(query: str, params: tuple = None, fetch: bool = True) -> tuple:
        """Generische Methode für Datenbankabfragen"""
        conn = None
        cursor = None
        try:
            conn = DatabaseManager.get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(query, params or ())
            
            if fetch:
                result = cursor.fetchall()
                return True, result
            
            conn.commit()
            return True, None
            
        except mysql.connector.Error as e:
            logger.error(f"Database error: {e}")
            return False, str(e)
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

def init_tables() -> None:
    """Initialisiert alle Datenbanktabellen"""
    tables = {
        'tasks': '''
            CREATE TABLE IF NOT EXISTS tasks (
                task_id INT AUTO_INCREMENT PRIMARY KEY,
                task_type VARCHAR(255),
                status VARCHAR(50),
                assigned_to VARCHAR(255),
                priority INT,
                details TEXT,
                fast_interval BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''',
        'logs': '''
            CREATE TABLE IF NOT EXISTS logs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                event_type VARCHAR(255),
                details TEXT,
                logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''',
        'Test': '''
            CREATE TABLE IF NOT EXISTS Test (
                id INT AUTO_INCREMENT PRIMARY KEY,
                Spalte1 TEXT NULL,
                Spalte2 TEXT NULL,
                Spalte3 TEXT NULL,
                Spalte4 TEXT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        '''
    }
    
    for table_name, create_statement in tables.items():
        success, error = DatabaseManager.execute_query(create_statement, fetch=False)
        if not success:
            logger.error(f"Failed to create table {table_name}: {error}")

# API-Routen

@app.before_request
def before_request():
    """Logging für alle Anfragen"""
    logger.info(f"Request: {request.method} {request.path}")
    logger.info(f"Headers: {dict(request.headers)}")
    if request.get_json(silent=True):
        logger.info(f"Body: {request.get_json()}")

@app.route('/init-db', methods=['GET'])
@require_auth
def init_db():
    """Initialisiert die Datenbank"""
    try:
        init_tables()
        return jsonify({"status": "success", "message": "Database initialized successfully"})
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/add_entry', methods=['POST'])
@require_auth
def add_entry():
    """Fügt einen Eintrag in eine beliebige Tabelle ein"""
    data = request.get_json()
    table = data.get('table')
    values = data.get('values')

    if not table or not values:
        return jsonify({"status": "error", "message": "Missing table or values"}), 400

    columns = ', '.join(values.keys())
    placeholders = ', '.join(['%s'] * len(values))
    query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
    
    success, result = DatabaseManager.execute_query(query, tuple(values.values()), fetch=False)
    
    if success:
        return jsonify({"status": "success", "message": "Entry added successfully"})
    return jsonify({"status": "error", "message": result}), 500

@app.route('/get_entries', methods=['GET'])
@require_auth
def get_entries():
    """Ruft alle Einträge aus einer Tabelle ab"""
    table = request.args.get('table')
    if not table:
        return jsonify({"status": "error", "message": "Table not specified"}), 400

    query = f"SELECT * FROM {table}"
    success, result = DatabaseManager.execute_query(query)
    
    if success:
        return jsonify({"status": "success", "data": result})
    return jsonify({"status": "error", "message": result}), 500

@app.route('/update_task_status', methods=['POST'])
@require_auth
def update_task_status():
    """Aktualisiert den Status eines Tasks"""
    data = request.get_json()
    task_id = data.get('task_id')
    new_status = data.get('status')

    if not task_id or not new_status:
        return jsonify({"status": "error", "message": "Missing task_id or status"}), 400

    query = "UPDATE tasks SET status = %s WHERE task_id = %s"
    success, error = DatabaseManager.execute_query(query, (new_status, task_id), fetch=False)
    
    if success:
        return jsonify({"status": "success", "message": "Task status updated successfully"})
    return jsonify({"status": "error", "message": error}), 500

@app.route('/get_pending_tasks', methods=['GET'])
@require_auth
def get_pending_tasks():
    """Ruft alle ausstehenden Tasks ab"""
    query = "SELECT * FROM tasks WHERE status = 'pending' ORDER BY priority DESC, created_at ASC"
    success, result = DatabaseManager.execute_query(query)
    
    if success:
        return jsonify({"status": "success", "tasks": result})
    return jsonify({"status": "error", "message": result}), 500

@app.route('/get_high_priority_tasks', methods=['GET'])
@require_auth
def get_high_priority_tasks():
    """Ruft alle hochprioritären Tasks ab"""
    query = """
        SELECT * FROM tasks 
        WHERE fast_interval = TRUE AND status = 'pending'
        ORDER BY priority DESC, created_at ASC
    """
    success, result = DatabaseManager.execute_query(query)
    
    if success:
        return jsonify({"status": "success", "tasks": result})
    return jsonify({"status": "error", "message": result}), 500

@app.route('/log_event', methods=['POST'])
@require_auth
def log_event():
    """Protokolliert ein Event"""
    data = request.get_json()
    event_type = data.get('event_type')
    details = data.get('details')

    if not event_type or not details:
        return jsonify({"status": "error", "message": "Missing event_type or details"}), 400

    query = "INSERT INTO logs (event_type, details) VALUES (%s, %s)"
    success, error = DatabaseManager.execute_query(query, (event_type, details), fetch=False)
    
    if success:
        return jsonify({"status": "success", "message": "Event logged successfully"})
    return jsonify({"status": "error", "message": error}), 500

@app.route('/test-insert-and-fetch', methods=['POST'])
@require_auth
def test_insert_and_fetch():
    """Fügt Testdaten in die Tabelle 'Test' ein und ruft sie ab"""
    try:
        test_data = {
            "Spalte1": "Wert1",
            "Spalte2": "Wert2",
            "Spalte3": "Wert3",
            "Spalte4": "Wert4"
        }
        
        columns = ', '.join(test_data.keys())
        placeholders = ', '.join(['%s'] * len(test_data))
        query = f"INSERT INTO Test ({columns}) VALUES ({placeholders})"
        
        success, error = DatabaseManager.execute_query(query, tuple(test_data.values()), fetch=False)
        if not success:
            return jsonify({"status": "error", "message": error}), 500

        query = "SELECT * FROM Test"
        success, result = DatabaseManager.execute_query(query)
        if not success:
            return jsonify({"status": "error", "message": result}), 500

        return jsonify({"status": "success", "inserted_data": test_data, "fetched_data": result})

    except Exception as e:
        logger.error(f"Error in test_insert_and_fetch: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# Error Handler
@app.errorhandler(Exception)
def handle_error(error):
    """Globaler Error Handler"""
    logger.error(f"Unhandled error: {str(error)}")
    return jsonify({
        "status": "error",
        "message": "An unexpected error occurred",
        "error": str(error)
    }), 500

if __name__ == '__main__':
    # Starten Sie die Anwendung mit SSL im Produktionsmodus
    app.run(
        host='0.0.0.0',
        port=int(os.getenv('PORT', 5000)),
        ssl_context='adhoc' if os.getenv('ENVIRONMENT') == 'production' else None
    )