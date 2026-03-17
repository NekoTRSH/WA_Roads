from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from sqlalchemy import create_engine, text

LAYER_URL = os.getenv(
    "MAINROADS_ROAD_NETWORK_URL",
    "https://gisservices.mainroads.wa.gov.au/arcgis/rest/services/OpenData/RoadAssets_DataPortal/MapServer/17"
)

RAW_DIR = Path(os.getenv("RAW_ROAD_DIR", "data/raw/road_network"))
SILVER_DIR = Path(os.getenv("SILVER_DIR", "data/silver"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "2000"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "60"))
DATABASE_URL = os.getenv("DATABASE_URL")

session = requests.Session()

