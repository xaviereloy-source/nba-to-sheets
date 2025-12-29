import os
import json
import datetime
import pandas as pd
import time

from nba_api.stats.endpoints import leaguegamefinder, boxscoretraditionalv2
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ======================
# CONFIG
# ======================
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
SHEET_NAME = "Player_Game_Stats"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ======================
# GOOGLE AUTH
# ======================
with open("google-credentials.json", "w") as f:
    f.write(os.environ["GOOGLE_CREDENTIALS"])

creds = Credentials.from_service_account_file(
    "google-credentials.json", scopes=SCOPES
)
service = build("sheets", "v4", credentials=creds)
sheet = service.spreadsheets()

# ======================
# NBA FUNCTIONS
# ======================
def get_today_games():
    today = 2024-03-15

    games = leaguegamefinder.LeagueGameFinder(
        season_nullable="2025-26",
        season_type_nullable="Regular Season"
    ).get_data_frames()[0]

    return games[games["GAME_DATE"] == today]


def get_players_stats(game_id):
    boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(
        game_id=game_id
    )

    players = boxscore.get_data_frames()[0]
    return players


# ======================
# WRITE TO SHEETS
# ======================
def append_to_sheet(df):
    values = df.astype(str).values.tolist()

    sheet.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": values}
    ).execute()


# ======================
# MAIN
# ======================
if __name__ == "__main__":
    today = datetime.date.today().strftime("%Y-%m-%d")
    games_today = get_today_games()

    if games_today.empty:
        print("Aucun match aujourd’hui.")
        exit()

    all_players = []

    for game_id in games_today["GAME_ID"].unique():
        print(f"Match {game_id}")
        time.sleep(1.2)  # IMPORTANT pour éviter le rate limit NBA API

        players = get_players_stats(game_id)

        players = players[[
            "GAME_ID", "GAME_DATE", "PLAYER_ID", "PLAYER_NAME",
            "TEAM_ID", "TEAM_ABBREVIATION", "MIN", "PTS",
            "REB", "AST", "STL", "BLK", "TOV",
            "FGM", "FGA", "FG_PCT",
            "FG3M", "FG3A", "FG3_PCT",
            "FTM", "FTA", "FT_PCT", "PLUS_MINUS"
        ]]

        players["IMPORT_DATE"] = today
        all_players.append(players)

    final_df = pd.concat(all_players)
    final_df = final_df[[
        "IMPORT_DATE", "GAME_DATE", "GAME_ID",
        "PLAYER_ID", "PLAYER_NAME",
        "TEAM_ID", "TEAM_ABBREVIATION",
        "MIN", "PTS", "REB", "AST", "STL", "BLK", "TOV",
        "FGM", "FGA", "FG_PCT",
        "FG3M", "FG3A", "FG3_PCT",
        "FTM", "FTA", "FT_PCT", "PLUS_MINUS"
    ]]

    append_to_sheet(final_df)
    print(f"{len(final_df)} lignes joueurs ajoutées.")
