import os
import json
import datetime
import time
import sys
import pandas as pd

from requests.exceptions import ReadTimeout
from nba_api.stats.endpoints import leaguegamefinder, boxscoretraditionalv2
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


# ======================================================
# CONFIGURATION
# ======================================================
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
SHEET_NAME = "Player_Game_Stats"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


# ======================================================
# GOOGLE AUTHENTICATION
# ======================================================
# Le fichier google-credentials.json est créé par GitHub Acti
