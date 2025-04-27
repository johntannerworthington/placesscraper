PlacesScraper

A simple, fast, and open-source tool to generate location-based search queries and scrape Google Places data via the Serper.dev Places API.

Features
- Upload a list of cities and base queries to combine search terms.
- Scrape real business listings based on your combined queries.
- Outputs a clean, deduplicated CSV of places with:
  - Business name
  - Address
  - Phone number
  - Website
  - Rating and review count
  - Direct Google Maps links (built automatically from CIDs)
- Normalizes special characters (e.g., n instead of ñ, e instead of é) for clean English readability.
- Handles pagination and API retries automatically.
- Fully open-source — run it locally or use the hosted version.

Technologies Used
- Python 3.10+
- Flask (web app)
- Requests (API calls)
- Gunicorn (production WSGI server)
- Render.com (cloud hosting)

Getting Started (Local Setup)

1. Clone the Repo
   git clone https://github.com/johntannerworthington/placesscraper.git
   cd placesscraper

2. Install Dependencies
   pip install -r requirements.txt

3. Run Locally
   python app.py

Then visit http://localhost:5000 in your browser to use the app.

Hosted Deployment (Render.com)
You can also use the hosted version deployed on Render.com (coming soon).

Render Setup:
- Render reads the render.yaml blueprint.
- Choose the Starter-Plus Plan ($25/month) for best performance.
- App automatically builds and deploys from GitHub.

Project Structure
/app.py                 - Flask web server
/combine.py             - Combine cities and queries
/serper_combined.py     - Scraper for Places API
/static/                - Example CSVs for users
/uploads/               - Uploaded files and outputs (created at runtime)
/index.html             - Frontend user form
/requirements.txt       - Python dependencies
/render.yaml            - Render.com deployment config
/README.md              - This file

API Key Requirement
This app requires a Serper.dev API Key.
Users provide their own API key through the web form — the key is never stored server-side.
Sign up for a free or paid API key at https://serper.dev.

Security & Privacy
- No user data or API keys are stored.
- No databases involved.
- Files are processed in memory and served directly back to the user.

Contributing
Pull requests, feature ideas, and feedback are welcome.
If you find this tool useful, please consider starring the repo to help others discover it.

License
This project is licensed under the MIT License — free to use, modify, and distribute.

Contact
Created by John Tanner Worthington (https://www.cold.email).
Feel free to reach out or open an issue if you encounter any problems.
