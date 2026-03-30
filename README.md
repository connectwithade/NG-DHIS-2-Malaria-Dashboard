# Render Deployment

This folder is a self-contained Render deployment package for the DHIS2 Nigeria Malaria Indicator Dashboard.

## Files

- `app.py`: Flask application
- `static/`: dashboard assets
- `requirements.txt`: Python dependencies
- `render.yaml`: optional Render Blueprint definition
- `.env.example`: required environment variables
- `malaria_indicator_linkage_live.csv`: validated malaria indicator mapping
- `dhis2_sync_state.json`: sync state seed file

## Quick Deploy on Render

### Option 1: Standard Web Service

1. Push this folder to GitHub.
2. In Render, create a new **Web Service** from that repository.
3. Use:
   - Build command: `pip install -r requirements.txt`
   - Start command: `gunicorn app:app`
4. Add environment variables:
   - `DHIS2_BASE`
   - `DHIS2_USER`
   - `DHIS2_PASS`
   - `DHIS2_TIMEOUT`

### Option 2: Blueprint

1. Push this folder to GitHub.
2. In Render, create a new **Blueprint** service from the repository.
3. Render will read `render.yaml`.
4. After creation, set the secret values for:
   - `DHIS2_USER`
   - `DHIS2_PASS`

## Notes

- Do not hardcode DHIS2 credentials in the repository.
- Render supports outbound internet access, so it avoids the PythonAnywhere free-tier restriction you hit.
- Once deployed, use the Render URL directly or embed that URL in SharePoint.
