# WA Road and Traffic Congestion 

Traffic in Western Australia, specifically Perth can sometimes be overly complicated and tedious to sit through. This repo/program will attempt to analyse road and traffic data (live hopefully) to get a better understanding in what makes a road congested and the reasons it does so.

## Run It (Step-by-step)

### 0) Prereqs
- Python 3.10+ (3.11 recommended)
- Docker Desktop (for PostGIS + pgAdmin)

### 1) Create/activate a virtualenv + install deps
```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

### 2) Start PostGIS + pgAdmin
```bash
docker compose up -d
docker compose ps
```

- PostGIS runs on `localhost:5432`
- pgAdmin runs on `http://localhost:5051`
- Credentials are in `.env`

Note: the database schema is created from `sql/ddl/*.sql` **only the first time** the `postgis_data` volume is created. If you already ran the DB before these SQL files existed/changed, recreate the volume:
```bash
docker compose down -v
docker compose up -d
```

### 3) Ingest the Main Roads road network (writes Parquet)
This downloads the Road Network layer from Main Roads’ ArcGIS endpoint and writes:
- raw batch files to `data/raw/road_network/<run_id>/...`
- a “silver” parquet file to `data/silver/road_network_<run_id>.parquet`

```bash
python3 src/ingest/mainroads_api.py
```

Optional env vars:
- `MAINROADS_ROAD_NETWORK_URL` (override the ArcGIS layer URL)
- `BATCH_SIZE` (default `2000`)
- `HTTP_TIMEOUT` (default `60`)
- `RAW_ROAD_DIR` (default `data/raw/road_network`)
- `SILVER_DIR` (default `data/silver`)

### 4) Load latest Parquet into PostGIS
```bash
python3 src/load/postgres.py
```

This truncates and reloads `core.road_segment`.

DB connection: scripts will use `DATABASE_URL` if set, otherwise they build a URL from `.env` (`POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_HOST`, `POSTGRES_PORT`).

### 5) Build mart table used by the dashboard
```bash
docker exec -i wa_roads_postgis psql -U wa_admin -d wa_roads < sql/transforms/lga_road_stats.sql
```

### 6) Run the Streamlit dashboard
```bash
streamlit run dashboards/streamlit_app.py
```

Open the URL Streamlit prints (usually `http://localhost:8501`).
