import os
import time
from datetime import datetime
import pandas as pd

from nba_api.stats.endpoints import leaguegamefinder, boxscoretraditionalv3
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


# =========================
# CONFIG
# =========================

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
SHEET_NAME = "Player_Game_Stats"

SEASON = "2025-26"

MAX_RETRIES = 3
SLEEP_BETWEEN_CALLS = 3
SLEEP_BETWEEN_RETRIES = 10


# =========================
# GOOGLE SHEETS
# =========================

def get_sheets_service():
    creds = Credentials.from_service_account_file(
        "google-credentials.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds)


def append_to_sheet(service, df):
    if df.empty:
        print("Aucune donnée à écrire dans Google Sheets.")
        return

    values = [df.columns.tolist()] + df.astype(str).values.tolist()

    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": values}
    ).execute()

    print(f"{len(df)} lignes ajoutées dans Google Sheets.")


# =========================
# NBA DATA
# =========================

def get_today_games():
    today = datetime.utcnow().strftime("%Y-%m-%d")
    print(f"Recherche des matchs NBA pour la date : {today}")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            games = leaguegamefinder.LeagueGameFinder(
                season_nullable=SEASON,
                date_from_nullable=today,
                date_to_nullable=today,
                league_id_nullable="00"
            ).get_data_frames()[0]

            if games.empty:
                print("Aucun match NBA aujourd’hui.")
                return pd.DataFrame()

            print(f"{len(games)} matchs trouvés aujourd’hui.")
            return games

        except Exception as e:
            print(f"Erreur API NBA (tentative {attempt}) : {e}")
            time.sleep(SLEEP_BETWEEN_RETRIES)

    print("API NBA indisponible après plusieurs tentatives.")
    return pd.DataFrame()


def get_players_stats(game_id):
    time.sleep(SLEEP_BETWEEN_CALLS)

    boxscore = boxscoretraditionalv3.BoxScoreTraditionalV3(
        game_id=game_id
    )

    df = boxscore.get_data_frames()[0]
    df["GAME_ID"] = game_id
    return df


# =========================
# MAIN
# =========================

if __name__ == "__main__":
    print("Démarrage du script NBA Player Stats (matchs du jour)")

    games_today = get_today_games()
    if games_today.empty:
        print("Aucun match à traiter. Fin du script.")
        exit(0)

    service = get_sheets_service()
    all_players = []

    for game_id in games_today["GAME_ID"].unique():
        try:
            print(f"Traitement du match {game_id}")
            df_players = get_players_stats(game_id)
            all_players.append(df_players)
        except Exception as e:
            print(f"Erreur sur le match {game_id} : {e}")

    if not all_players:
        print("Aucune statistique joueur récupérée.")
        exit(0)

    final_df = pd.concat(all_players, ignore_index=True)
    append_to_sheet(service, final_df)

    print("Script terminé avec succès.")
