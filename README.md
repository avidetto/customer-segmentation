# Customer Segmentation Databricks App

This project is a packaged Databricks application that performs customer segmentation using clustering on Delta purchase data and exposes a React UI for exploring segment results.

## Structure

- `app/` - Python backend entrypoint and segmentation pipeline
- `web/` - React frontend
- `databricks-app.json` - Databricks app metadata manifest

## Backend

The Python backend exposes a FastAPI service that reads a source Delta table, builds features from purchase patterns and geography, runs clustering with `pyspark.ml`, and writes segment assignments to an output Delta table.

### Build locally

**Prerequisites:** Node.js 18+ and npm installed on your machine

1. Build the React frontend:
   ```bash
   cd web
   npm install
   npm run build
   cd ..
   ```
   ✅ **Build completed** - The `web/dist` directory contains the production build.

2. Install backend dependencies:
   ```bash
   python -m pip install -r app/requirements.txt
   ```
3. Start the API server:
   ```bash
   python app/server.py
   ```
4. The frontend will be served at `http://localhost:8000`

### Run on Databricks

1. Ensure you have built the frontend locally (see above)
2. Commit the `web/dist` directory to git
3. Deploy the app to Databricks—it will pull the pre-built frontend and start immediately

The Databricks build process will attempt to rebuild the frontend if Node.js is available in the runtime environment.

## Frontend

The React app loads segment summaries and cluster details. In Databricks deployment, build output from `web/dist` can be served as static assets.

## Files

- `app/main.py` - command-line entrypoint
- `app/segmentation.py` - Spark ingestion, feature preparation, clustering, and output writing
- `web/src/App.jsx` - UI shell and interactive settings
- `web/src/components/SegmentSummary.jsx` - segment detail renderer
- `databricks-app.json` - manifest for packaged deployment

## Dependencies

- Python: `pyspark`, `pandas`
- Frontend: `react`, `react-dom`, `vite`

## Notes

The app is designed for Databricks runtime with Delta Lake support. The backend assumes the source and target tables are accessible in the current Spark catalog.
