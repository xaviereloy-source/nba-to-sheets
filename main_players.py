import time
from datetime import datetime
from nba_api.stats.endpoints import leaguegamefinder, boxscoretraditionalv3
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# =============================
# CONFIG
# =============================
SPREADSHEET_ID = "TON_SPREADSHEET_ID"
SHEET_NAME = "Player_Game_Stats"

SLEEP_BETWEEN_CALLS = 2  # IMPORTANT pour éviter blocage NBA

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
        print("Aucune donnée à écrire.")
        return

    sheets_service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows}
    ).execute()


def get_today_games():
    today = datetime.today().strftime("%Y-%m-%d")
    print(f"Recherche des matchs NBA pour {today}")

    games = leaguegamefinder.LeagueGameFinder(
        date_from_nullable=today,
        date_to_nullable=today
    ).get_data_frames()[0]

    return games


# =============================
# MAIN
# =============================
print("Démarrage script NBA Player Stats (NAS)")

games = get_today_games()

if games.empty:
    print("Aucun match aujourd'hui.")
    exit(0)

rows = []

for _, game in games.iterrows():
    game_id = game["GAME_ID"]
    game_date = game["GAME_DATE"]

    print(f"Match {game_id} — récupération stats joueurs")

    boxscore = boxscoretraditionalv3.BoxScoreTraditionalV3(
        game_id=game_id
    ).get_data_frames()[0]

    for _, row in boxscore.iterrows():
        rows.append([
            game_date,
            game_id,
            row["playerId"],
            row["playerName"],
            row["teamAbbreviation"],
            row["points"],
            row["reboundsTotal"],
            row["assists"],
            row["steals"],
            row["blocks"],
            row["turnovers"],
            row["minutes"]
        ])

    time.sleep(SLEEP_BETWEEN_CALLS)

append_rows(rows)

print(f"{len(rows)} lignes ajoutées dans Google Sheets")
print("Script terminé avec succès ✅")
