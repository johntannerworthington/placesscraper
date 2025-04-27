
from flask import Flask, request, send_file, render_template_string
import os
from combine import generate_combined_csv
from serper_combined import run_serper

app = Flask(__name__)

@app.route('/')
def index():
    with open('index.html') as f:
        return render_template_string(f.read())

@app.route('/combine', methods=['POST'])
def combine():
    cities_file = request.files['cities']
    queries_file = request.files['queries']

    os.makedirs('uploads', exist_ok=True)

    cities_path = 'uploads/cities.csv'
    queries_path = 'uploads/queries.csv'
    output_path = 'uploads/combined_queries.csv'
    uszips_path = 'uszips.csv'

    cities_file.save(cities_path)
    queries_file.save(queries_path)

    generate_combined_csv(cities_path, queries_path, uszips_path, output_path)

    return send_file(output_path, as_attachment=True)

@app.route('/serper', methods=['POST'])
def serper():
    queries_file = request.files['queries']
    serper_api_key = request.form['serper_api_key']

    os.makedirs('uploads', exist_ok=True)
    queries_path = 'uploads/queries.csv'
    queries_file.save(queries_path)

    output_path = run_serper(queries_path, serper_api_key)

    return send_file(output_path, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
