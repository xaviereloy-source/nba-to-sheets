import os
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
SHEET_NAME = "Player_Game_Stats"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

creds = Credentials.from_service_account_file(
    "google-credentials.json",
    scopes=SCOPES
)

service = build("sheets", "v4", credentials=creds)
sheet = service.spreadsheets()

print("Test écriture Google Sheets")

values = [
    ["TEST", "2024-03-15", "GAME_TEST", "0", "JOUEUR TEST", "0", "TST", "10", "20", "5", "7", "1", "0", "2",
     "8", "15", "0.53", "2", "5", "0.40", "2", "2", "1.00", "+5"]
]

sheet.values().append(
    spreadsheetId=SPREADSHEET_ID,
    range=f"{SHEET_NAME}!A1",
    valueInputOption="RAW",
    insertDataOption="INSERT_ROWS",
    body={"values": values}
).execute()

print("Ligne de test ajoutée avec succès")
