import os
import json
import time
import requests
from datetime import datetime

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# =============================
# CONFIG
# =============================
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
SHEET_NAME = "Player_Game_Stats"
BALDONTLIE_BASE_URL = "https://api.balldontlie.io/v1"

# =============================
# GOOGLE SHEETS AUTH
# =============================
creds = Credentials.from_service_account_file(
    "google-credentials.json",
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)

sheets_service = build("sheets", "v4", credentials=creds)

# =============================
# HELPERS
# =============================
def append_rows(rows):
    if not rows:
        return

    sheets_service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows}
    ).execute()


def get_games_today(date_str):
    url = f"{BALDONTLIE_BASE_URL}/games"
    params = {
        "dates[]": date_str,
        "per_page": 100
    }
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()["data"]


def get_game_stats(game_id):
    url = f"{BALDONTLIE_BASE_URL}/stats"
    params = {
        "game_ids[]": game_id,
        "per_page": 100
    }

    all_stats = []
    page = 1

    while True:
        params["page"] = page
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()

        all_stats.extend(data["data"])

        if data["meta"]["next_page"] is None:
            break
        page += 1
        time.sleep(0.3)

    return all_stats


# =============================
# MAIN
# =============================
print("Démarrage du script NBA Player Stats (balldontlie)")

today = datetime.utcnow().strftime("%Y-%m-%d")
print(f"Recherche des matchs NBA pour : {today}")

try:
    games = get_games_today(today)
except Exception as e:
    print("Erreur récupération matchs :", e)
    games = []

if not games:
    print("Aucun match aujourd'hui. Fin du script.")
    exit(0)

rows_to_insert = []

for game in games:
    game_id = game["id"]
    game_date = game["date"][:10]

    print(f"Match ID {game_id} — récupération stats joueurs")

    try:
        stats = get_game_stats(game_id)
    except Exception as e:
        print(f"Erreur stats match {game_id} :", e)
        continue

    for s in stats:
        player = s["player"]
        team = s["team"]

        rows_to_insert.append([
            game_date,
            game_id,
            player["id"],
            f'{player["first_name"]} {player["last_name"]}',
            team["abbreviation"],
            s["pts"],
            s["reb"],
            s["ast"],
            s["stl"],
            s["blk"],
            s["turnover"],
            s["min"]
        ])

    time.sleep(0.5)

append_rows(rows_to_insert)

print(f"{len(rows_to_insert)} lignes ajoutées dans Google Sheets")
print("Script terminé avec succès ✅")
