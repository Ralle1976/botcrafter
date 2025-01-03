import os
import logging
import mysql.connector
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# Logging konfigurieren
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Umgebungsvariablen laden
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

# Flask-App erstellen
app = Flask(__name__)
app.debug = True

# API-Token für die Autorisierung
API_TOKEN = os.getenv("API_TOKEN")

# MySQL-Konfiguration
db_config = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

# Globale Debugging-Funktion
def log_request_details():
    headers = dict(request.headers)
    body = request.get_json(silent=True)
    args = request.args.to_dict()

    logger.info("Headers: %s", headers)
    logger.info("Body: %s", body)
    logger.info("Args: %s", args)

@app.before_request
def before_request():
    log_request_details()  # Loggt alle Anfragen
    token = request.headers.get("Authorization")
    if token != API_TOKEN:
        return jsonify({"status": "error", "message": "Unauthorized"}), 401

@app.route('/test', methods=['GET', 'POST'])
def test_route():
    return jsonify({"status": "success", "message": "Test erfolgreich"})

@app.route('/db_test', methods=['GET'])
def db_test():
    try:
        conn = mysql.connector.connect(**db_config)
        return jsonify({"status": "success", "message": "Datenbankverbindung erfolgreich"})
    except mysql.connector.Error as e:
        logger.error(f"Fehler bei der Datenbankverbindung: {str(e)}")
        return jsonify({"status": "error", "message": f"Fehler bei der Datenbankverbindung: {str(e)}"}), 500
    finally:
        if 'conn' in locals() and conn:
            conn.close()

@app.route('/add_entry', methods=['POST'])
def add_entry():
    data = request.get_json()
    table = data.get('table')
    values = data.get('values')

    if not table or not values:
        return jsonify({"status": "error", "message": "Tabelle oder Werte fehlen"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        placeholders = ', '.join(['%s'] * len(values))
        columns = ', '.join(values.keys())
        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        cursor.execute(sql, list(values.values()))
        conn.commit()

        return jsonify({"status": "success", "message": "Eintrag erfolgreich hinzugefügt"})

    except mysql.connector.Error as e:
        logger.error(f"Fehler beim Hinzufügen zur Datenbank: {str(e)}")
        return jsonify({"status": "error", "message": f"Fehler: {str(e)}"}), 500

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

@app.route('/get_entries', methods=['GET'])
def get_entries():
    table = request.args.get('table')

    if not table:
        return jsonify({"status": "error", "message": "Tabelle nicht angegeben"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        sql = f"SELECT * FROM {table}"
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return jsonify({"status": "success", "data": [dict(zip(columns, row)) for row in rows]})

    except mysql.connector.Error as e:
        logger.error(f"Fehler beim Abrufen der Einträge: {str(e)}")
        return jsonify({"status": "error", "message": f"Fehler: {str(e)}"}), 500

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

@app.route('/update_task_status', methods=['POST'])
def update_task_status():
    data = request.get_json()
    task_id = data.get('task_id')
    new_status = data.get('status')

    if not task_id or not new_status:
        return jsonify({"status": "error", "message": "Task-ID oder Status fehlt"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        sql = "UPDATE tasks SET status = %s WHERE task_id = %s"
        cursor.execute(sql, (new_status, task_id))
        conn.commit()

        return jsonify({"status": "success", "message": "Task-Status aktualisiert"})
    except mysql.connector.Error as e:
        logger.error(f"Fehler beim Aktualisieren des Task-Status: {str(e)}")
        return jsonify({"status": "error", "message": f"Fehler: {str(e)}"}), 500
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

@app.route('/get_pending_tasks', methods=['GET'])
def get_pending_tasks():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        sql = "SELECT * FROM tasks WHERE status = 'pending'"
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return jsonify({"status": "success", "tasks": [dict(zip(columns, row)) for row in rows]})
    except mysql.connector.Error as e:
        logger.error(f"Fehler beim Abrufen der ausstehenden Aufgaben: {str(e)}")
        return jsonify({"status": "error", "message": f"Fehler: {str(e)}"}), 500
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

@app.route('/add_task', methods=['POST'])
def add_task():
    data = request.get_json()
    task_type = data.get('task_type')
    assigned_to = data.get('assigned_to')
    priority = data.get('priority', 1)
    details = data.get('details', '')

    if not task_type or not assigned_to:
        return jsonify({"status": "error", "message": "Task-Typ oder zugewiesener GPT fehlt"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        sql = """
            INSERT INTO tasks (task_type, status, assigned_to, priority, details)
            VALUES (%s, 'pending', %s, %s, %s)
        """
        cursor.execute(sql, (task_type, assigned_to, priority, details))
        conn.commit()

        return jsonify({"status": "success", "message": "Task erfolgreich hinzugefügt"})
    except mysql.connector.Error as e:
        logger.error(f"Fehler beim Hinzufügen des Tasks: {str(e)}")
        return jsonify({"status": "error", "message": f"Fehler: {str(e)}"}), 500
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

@app.route('/mark_task_intensive', methods=['POST'])
def mark_task_intensive():
    data = request.get_json()
    task_id = data.get('task_id')

    if not task_id:
        return jsonify({"status": "error", "message": "Task-ID fehlt"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        sql = "UPDATE tasks SET fast_interval = TRUE WHERE task_id = %s"
        cursor.execute(sql, (task_id,))
        conn.commit()
        return jsonify({"status": "success", "message": "Task als intensiv markiert"})
    except mysql.connector.Error as e:
        logger.error(f"Fehler beim Markieren des Tasks: {str(e)}")
        return jsonify({"status": "error", "message": f"Fehler: {str(e)}"}), 500
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

@app.route('/get_high_priority_tasks', methods=['GET'])
def get_high_priority_tasks():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        sql = "SELECT * FROM tasks WHERE fast_interval = TRUE AND status = 'pending'"
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return jsonify({"status": "success", "tasks": [dict(zip(columns, row)) for row in rows]})
    except mysql.connector.Error as e:
        logger.error(f"Fehler beim Abrufen der priorisierten Aufgaben: {str(e)}")
        return jsonify({"status": "error", "message": f"Fehler: {str(e)}"}), 500
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

@app.route('/get_logs', methods=['GET'])
def get_logs():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        sql = "SELECT * FROM logs ORDER BY logged_at DESC LIMIT 100"
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return jsonify({"status": "success", "logs": [dict(zip(columns, row)) for row in rows]})
    except mysql.connector.Error as e:
        logger.error(f"Fehler beim Abrufen der Logs: {str(e)}")
        return jsonify({"status": "error", "message": f"Fehler: {str(e)}"}), 500
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

@app.route('/log_event', methods=['POST'])
def log_event():
    data = request.get_json()
    event_type = data.get('event_type')
    details = data.get('details')

    if not event_type or not details:
        return jsonify({"status": "error", "message": "Event-Typ oder Details fehlen"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                event_type VARCHAR(255),
                details TEXT,
                logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("INSERT INTO logs (event_type, details) VALUES (%s, %s)", (event_type, details))
        conn.commit()
        return jsonify({"status": "success", "message": "Event erfolgreich protokolliert"})
    except mysql.connector.Error as e:
        logger.error(f"Fehler beim Loggen des Events: {str(e)}")
        return jsonify({"status": "error", "message": f"Fehler: {str(e)}"}), 500
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

@app.route('/get_map_data', methods=['GET'])
def get_map_data():
    map_id = request.args.get('map_id')
    if not map_id:
        return jsonify({"status": "error", "message": "Map-ID fehlt"}), 400
    try:
        # Dynamische Datenbankabfrage (Platzhalter)
        map_data = {}  # Hier könnte später eine Funktion wie get_map_data_from_db(map_id) aufgerufen werden
        if not map_data:  # Prüfen, ob Daten vorhanden sind
            return jsonify({"status": "success", "map_data": []})
        return jsonify({"status": "success", "map_data": map_data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/submit_pathfinding_result', methods=['POST'])
def submit_pathfinding_result():
    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "Keine Daten bereitgestellt"}), 400
    try:
        # Hier könnten Pathfinding-Daten in die Datenbank gespeichert werden (Platzhalter)
        return jsonify({"status": "success", "message": "Pathfinding-Ergebnisse erfolgreich gespeichert."})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/get_task_status', methods=['GET'])
def get_task_status():
    try:
        # Beispiel einer dynamischen Statusabfrage (Platzhalter)
        tasks = []  # Platzhalter für Datenbankabfrage
        return jsonify({"status": "success", "tasks": tasks})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/documentation_update', methods=['POST'])
def documentation_update():
    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "Keine Daten bereitgestellt"}), 400
    try:
        # Hier könnten Dokumentationsupdates gespeichert werden (Platzhalter)
        return jsonify({"status": "success", "message": "Dokumentation erfolgreich aktualisiert."})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

