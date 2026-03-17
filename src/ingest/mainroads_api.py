from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from sqlalchemy import create_engine, text

