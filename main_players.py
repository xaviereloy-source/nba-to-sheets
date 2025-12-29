from nba_api.library.http import NBAStatsHTTP

NBAStatsHTTP.DEFAULT_HEADERS = {
    "Host": "stats.nba.com",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
}

import os
import datetime
import time
import sys
import pandas as pd

from requests.exceptions import ReadTimeout
from nba_api.stats.endpoints import leaguegamefinder, boxscoretraditionalv2
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# -----------------------------
# CONFIGURATION
# -----------------------------
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
SHEET_NAME = "Player_Game_Stats"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# -----------------------------
# GOOGLE AUTHENTICATION
# -----------------------------
creds = Credentials.from_service_account_file(
    "google-credentials.json",
    scopes=SCOPES
)
service = build("sheets", "v4", credentials=creds)
sheet = service.spreadsheets()

# -----------------------------
# FUNCTIONS NBA
# -----------------------------
def get_nba_season(date):
    """Retourne la saison NBA au format YYYY-YY selon la date"""
    year = date.year
    month = date.month
    if month >= 10:  # Oct, Nov, Dec → début de saison
        return f"{year}-{str(year+1)[2:]}"
    else:  # Jan → Juin
        return f"{year-1}-{str(year)[2:]}"

def get_today_games():
    today = datetime.date.today()
    season = get_nba_season(today)

    try:
        games = leaguegamefinder.LeagueGameFinder(
            season_nullable=season,
            season_type_nullable="Regular Season",
            timeout=300
        ).get_data_frames()[0]

    except ReadTimeout:
        print("API NBA indisponible (timeout). On réessaiera demain.")
        sys.exit(0)

    games["GAME_DATE"] = games["GAME_DATE"].astype(str)
    today_str = today.strftime("%Y-%m-%d")
    games_today = games[games["GAME_DATE"].str.startswith(today_str)]
    return games_today

def get_players_stats(game_id):
    try:
        boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(
            game_id=game_id,
            timeout=300
        )
        players = boxscore.get_data_frames()[0]
        return players
    except ReadTimeout:
        print(f"Timeout sur le match {game_id}, ignoré.")
        return None

# -----------------------------
# GOOGLE SHEETS
# -----------------------------
def append_to_sheet(df):
    values = df.astype(str).values.tolist()
    sheet.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": values}
    ).execute()

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":

    print("Démarrage du script NBA Player Stats")

    games_today = get_today_games()

    if games_today.empty:
        print("Aucun match NBA aujourd'hui.")
        sys.exit(0)

    all_players = []

    for game_id in games_today["GAME_ID"].unique():
        print(f"Traitement du match {game_id}")
        time.sleep(2)  # anti-blocage API

        players = get_players_stats(game_id)
        if players is None or players.empty:
            continue

        players = players[[
            "GAME_ID", "GAME_DATE", "PLAYER_ID", "PLAYER_NAME",
            "TEAM_ID", "TEAM_ABBREVIATION",
            "MIN", "PTS", "REB", "AST", "STL", "BLK", "TOV",
            "FGM", "FGA", "FG_PCT",
            "FG3M", "FG3A", "FG3_PCT",
            "FTM", "FTA", "FT_PCT",
            "PLUS_MINUS"
        ]]

        players["IMPORT_DATE"] = datetime.date.today().strftime("%Y-%m-%d")
        all_players.append(players)

    if not all_players:
        print("Aucune stat joueur récupérée.")
        sys.exit(0)

    final_df = pd.concat(all_players, ignore_index=True)

    # Réorganisation des colonnes pour Google Sheets
    final_df = final_df[[
        "IMPORT_DATE", "GAME_DATE", "GAME_ID",
        "PLAYER_ID", "PLAYER_NAME",
        "TEAM_ID", "TEAM_ABBREVIATION",
        "MIN", "PTS", "REB", "AST", "STL", "BLK", "TOV",
        "FGM", "FGA", "FG_PCT",
        "FG3M", "FG3A", "FG3_PCT",
        "FTM", "FTA", "FT_PCT",
        "PLUS_MINUS"
    ]]

    append_to_sheet(final_df)

    print(f"{len(final_df)} lignes joueurs ajoutées avec succès.")
