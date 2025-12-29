import os
import sys
import time
import datetime
import pandas as pd
import requests

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from nba_api.stats.endpoints import leaguegamefinder, boxscoretraditionalv2
from nba_api.library import http

# ======================================================
# CONFIGURATION GOOGLE SHEETS
# ======================================================
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
SHEET_NAME = "Player_Game_Stats"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ======================================================
# CONFIGURATION HEADERS NBA (OBLIGATOIRE)
# ======================================================
session = requests.Session()
session.headers.update({
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
})

# Forcer nba_api à utiliser cette session
http.requests = session

# ======================================================
# AUTHENTIFICATION GOOGLE
# ======================================================
creds = Credentials.from_service_account_file(
    "google-credentials.json",
    scopes=SCOPES
)
service = build("sheets", "v4", credentials=creds)
sheet = service.spreadsheets()

# ======================================================
# FONCTIONS UTILITAIRES
# ======================================================
def get_nba_season(date: datetime.date) -> str:
    """Retourne la saison NBA au format YYYY-YY"""
    if date.month >= 10:
        return f"{date.year}-{str(date.year + 1)[2:]}"
    else:
        return f"{date.year - 1}-{str(date.year)[2:]}"


def get_today_games():
    today = datetime.date.today()
    today_str = today.strftime("%Y-%m-%d")
    season = get_nba_season(today)

    for attempt in range(1, 4):
        try:
            print(f"Tentative {attempt} - récupération des matchs NBA ({season})")
            games = leaguegamefinder.LeagueGameFinder(
                season_nullable=season,
                season_type_nullable="Regular Season",
                timeout=100
            ).get_data_frames()[0]

            games["GAME_DATE"] = games["GAME_DATE"].astype(str)
            games_today = games[games["GAME_DATE"].str.startswith(today_str)]
            return games_today

        except Exception as e:
            print(f"Erreur API NBA (tentative {attempt}) : {e}")
            time.sleep(20)

    print("API NBA indisponible après 3 tentatives. Abandon.")
    sys.exit(0)


def get_players_stats(game_id):
    for attempt in range(1, 4):
        try:
            boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(
                game_id=game_id,
                timeout=100
            )
            return boxscore.get_data_frames()[0]

        except Exception as e:
            print(f"Erreur boxscore {game_id} (tentative {attempt}) : {e}")
            time.sleep(15)

    print(f"Match {game_id} ignoré après 3 échecs.")
    return None


def append_to_sheet(df: pd.DataFrame):
    values = df.astype(str).values.tolist()
    sheet.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": values}
    ).execute()

# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":

    print("Démarrage du script NBA Player Stats")

    games_today = get_today_games()

    if games_today.empty:
        print("Aucun match NBA aujourd'hui.")
        sys.exit(0)

    all_players = []

    for game_id in games_today["GAME_ID"].unique():
        print(f"Traitement du match {game_id}")
        time.sleep(3)  # anti-blocage API

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
        print("Aucune statistique joueur récupérée.")
        sys.exit(0)

    final_df = pd.concat(all_players, ignore_index=True)

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
