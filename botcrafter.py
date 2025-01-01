import os
import logging
import mysql.connector
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# Logging konfigurieren
logging.basicConfig(level=logging.INFO)

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
        logging.error(f"Fehler bei der Datenbankverbindung: {str(e)}")
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
        logging.error(f"Fehler beim Hinzufügen zur Datenbank: {str(e)}")
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
        logging.error(f"Fehler beim Abrufen der Einträge: {str(e)}")
        return jsonify({"status": "error", "message": f"Fehler: {str(e)}"}), 500

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

@app.route('/create_table', methods=['POST'])
def create_table():
    data = request.get_json()
    table_name = data.get('table_name')
    columns = data.get('columns')

    if not table_name or not columns:
        return jsonify({"status": "error", "message": "Tabellenname oder Spaltenstruktur fehlt"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        columns_sql = ', '.join([f"{col} {dtype}" for col, dtype in columns.items()])
        sql = f"CREATE TABLE {table_name} ({columns_sql})"
        cursor.execute(sql)
        conn.commit()

        return jsonify({"status": "success", "message": f"Tabelle {table_name} erfolgreich erstellt"})

    except mysql.connector.Error as e:
        logging.error(f"Fehler beim Erstellen der Tabelle: {str(e)}")
        return jsonify({"status": "error", "message": f"Fehler: {str(e)}"}), 500

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

@app.route('/list_tables', methods=['GET'])
def list_tables():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        sql = "SHOW TABLES"
        cursor.execute(sql)
        tables = [table[0] for table in cursor.fetchall()]
        return jsonify({"status": "success", "tables": tables})

    except mysql.connector.Error as e:
        logging.error(f"Fehler beim Abrufen der Tabellenliste: {str(e)}")
        return jsonify({"status": "error", "message": f"Fehler: {str(e)}"}), 500

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

@app.route('/delete_table', methods=['POST'])
def delete_table():
    data = request.get_json()
    table_name = data.get('table_name')

    if not table_name:
        return jsonify({"status": "error", "message": "Tabellenname fehlt"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        sql = f"DROP TABLE IF EXISTS {table_name}"
        cursor.execute(sql)
        conn.commit()

        return jsonify({"status": "success", "message": f"Tabelle {table_name} erfolgreich gelöscht"})

    except mysql.connector.Error as e:
        logging.error(f"Fehler beim Löschen der Tabelle: {str(e)}")
        return jsonify({"status": "error", "message": f"Fehler: {str(e)}"}), 500

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

@app.route('/truncate_table', methods=['POST'])
def truncate_table():
    data = request.get_json()
    table_name = data.get('table_name')

    if not table_name:
        return jsonify({"status": "error", "message": "Tabellenname fehlt"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        sql = f"TRUNCATE TABLE {table_name}"
        cursor.execute(sql)
        conn.commit()

        return jsonify({"status": "success", "message": f"Tabelle {table_name} erfolgreich geleert"})

    except mysql.connector.Error as e:
        logging.error(f"Fehler beim Leeren der Tabelle: {str(e)}")
        return jsonify({"status": "error", "message": f"Fehler: {str(e)}"}), 500

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

@app.route('/backup_table', methods=['GET'])
def backup_table():
    table_name = request.args.get('table')

    if not table_name:
        return jsonify({"status": "error", "message": "Tabellenname fehlt"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        sql = f"SELECT * FROM {table_name}"
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        backup_data = [dict(zip(columns, row)) for row in rows]
        return jsonify({"status": "success", "backup": backup_data})

    except mysql.connector.Error as e:
        logging.error(f"Fehler beim Erstellen des Backups: {str(e)}")
        return jsonify({"status": "error", "message": f"Fehler: {str(e)}"}), 500

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

# Logging-Funktion für parallele Zugriffe
@app.route('/get_logs', methods=['GET'])
def get_logs():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        sql = "SELECT * FROM access_log ORDER BY timestamp DESC LIMIT 100"
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return jsonify({"status": "success", "logs": [dict(zip(columns, row)) for row in rows]})
    except mysql.connector.Error as e:
        logging.error(f"Fehler beim Abrufen der Logs: {str(e)}")
        return jsonify({"status": "error", "message": f"Fehler: {str(e)}"}), 500
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

# Individuelle SQL-Abfragen
@app.route('/custom_query', methods=['POST'])
def custom_query():
    data = request.get_json()
    query = data.get('query')

    if not query:
        return jsonify({"status": "error", "message": "SQL-Abfrage fehlt"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(query)
        if query.strip().lower().startswith("select"):
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return jsonify({"status": "success", "data": [dict(zip(columns, row)) for row in rows]})
        else:
            conn.commit()
            return jsonify({"status": "success", "message": "Abfrage erfolgreich ausgeführt"})
    except mysql.connector.Error as e:
        logging.error(f"Fehler bei der SQL-Abfrage: {str(e)}")
        return jsonify({"status": "error", "message": f"Fehler: {str(e)}"}), 500
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

@app.route('/validate_table', methods=['GET'])
def validate_table():
    table_name = request.args.get('table_name')

    if not table_name:
        return jsonify({"status": "error", "message": "Tabellenname fehlt"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        result = cursor.fetchone()
        if result:
            return jsonify({"status": "success", "message": f"Tabelle '{table_name}' existiert"})
        else:
            return jsonify({"status": "error", "message": f"Tabelle '{table_name}' existiert nicht"})
    except mysql.connector.Error as e:
        logging.error(f"Fehler bei der Tabellenvalidierung: {str(e)}")
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
        logging.error(f"Fehler beim Loggen des Events: {str(e)}")
        return jsonify({"status": "error", "message": f"Fehler: {str(e)}"}), 500
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

#if __name__ == '__main__':
#    app.run()
