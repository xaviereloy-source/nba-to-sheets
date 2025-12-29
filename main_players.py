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
# Le fichier google-credentials.json est créé par GitHub Actions
creds = Credentials.from_service_account_file(
    "google-credentials.json",
    scopes=SCOPES
)

service = build("sheets", "v4", credentials=creds)
sheet = service.spreadsheets()


# ======================================================
# NBA FUNCTIONS
# ======================================================
def get_today_games():
    """
    Récupère les matchs NBA du jour.
    Si l'API NBA ne répond pas, on sort proprement.
    """
    today = "2024-03-15"

    try:
        games = leaguegamefinder.LeagueGameFinder(
            season_nullable="2023-24",
            season_type_nullable="Regular Season",
            timeout=60  # plus tolérant
        ).get_data_frames()[0]

    except ReadTimeout:
        print("API NBA indisponible (timeout). On réessaiera demain.")
        sys.exit(0)  # sortie propre → workflow vert

    games_today = games[games["GAME_DATE"] == today]
    return games_today


def get_players_stats(game_id):
    """
    Récupère les stats joueurs pour un match donné.
    """
    try:
        boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(
            game_id=game_id,
            timeout=60
        )
        players = boxscore.get_data_frames()[0]
        return players

    except ReadTimeout:
        print(f"Timeout sur le match {game_id}, ignoré.")
        return None


# ======================================================
# GOOGLE SHEETS
# ======================================================
def append_to_sheet(df):
    """
    Ajoute les lignes à la fin de la feuille Google Sheets.
    """
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

    today = "2024-03-15"
    games_today = get_today_games()

    if games_today.empty:
        print("Aucun match NBA aujourd’hui.")
        sys.exit(0)

    all_players = []

    for game_id in games_today["GAME_ID"].unique():
        print(f"Traitement du match {game_id}")
        time.sleep(2)  # anti-blocage NBA API

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

        players["IMPORT_DATE"] = today
        all_players.append(players)

    if not all_players:
        print("Aucune stat joueur récupérée.")
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
