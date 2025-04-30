from flask import Flask, request, send_file, render_template_string
import os
from combine import generate_combined_csv
from serper_combined import run_serper

app = Flask(__name__)
UPLOADS_DIR = '/uploads'  # ✅ Use persistent disk

@app.route('/')
def index():
    with open('index.html') as f:
        return render_template_string(f.read())

@app.route('/combine', methods=['POST'])
def combine():
    cities_file = request.files['cities']
    queries_file = request.files['queries']

    os.makedirs(UPLOADS_DIR, exist_ok=True)

    cities_path = os.path.join(UPLOADS_DIR, 'cities.csv')
    queries_path = os.path.join(UPLOADS_DIR, 'queries.csv')
    uszips_path = 'uszips.csv'

    cities_file.save(cities_path)
    queries_file.save(queries_path)

    output_path = generate_combined_csv(cities_path, queries_path, uszips_path)

    return send_file(output_path, as_attachment=True)

@app.route('/serper', methods=['POST'])
def serper():
    queries_file = request.files['queries']
    serper_api_key = request.form['serper_api_key']

    os.makedirs(UPLOADS_DIR, exist_ok=True)
    queries_path = os.path.join(UPLOADS_DIR, 'queries.csv')
    queries_file.save(queries_path)

    output_path = run_serper(queries_path, serper_api_key)

    return send_file(output_path, as_attachment=True)

@app.route('/download/<session_id>')
def download_file(session_id):
    file_path = os.path.join(UPLOADS_DIR, session_id, 'output.csv')
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    else:
        return f"❌ File not found for session ID: {session_id}", 404

if __name__ == '__main__':
    app.run(debug=True)
