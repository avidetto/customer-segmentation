# Customer Segmentation Databricks App

This project is a packaged Databricks application that performs customer segmentation using clustering on Delta purchase data and exposes a React UI for exploring segment results.

## Structure

- `app/` - Python backend entrypoint and segmentation pipeline
- `web/` - React frontend
- `databricks-app.json` - Databricks app metadata manifest

## Backend

The Python backend exposes a FastAPI service that reads a source Delta table, builds features from purchase patterns and geography, runs clustering with `pyspark.ml`, and writes segment assignments to an output Delta table.

### Run locally

1. Install backend dependencies:
   ```bash
   python -m pip install -r app/requirements.txt
   ```
2. Start the API server:
   ```bash
   python app/server.py
   ```
3. Start the React development server:
   ```bash
   cd web
   npm install
   npm run dev
   ```

### Run on Databricks

1. Build the frontend:
   ```bash
   cd web
   npm install
   npm run build
   cd ..
   ```
2. Package the app assets for Databricks (example archive step is manual or via your deployment pipeline).
3. Deploy the app to Databricks and start it using the declared `app/server.py` entrypoint.

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
