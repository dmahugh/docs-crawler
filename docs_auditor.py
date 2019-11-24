"""Cloud SQL Documentation Auditor - proof of concept
"""
import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials


def gspread_client():
    """Return authorized gspread client for the Cloud SQL Docs Auditor
    spreadsheet.
    """
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        "credentials.json", scopes
    )
    return gspread.authorize(credentials)


def column_alpha(col):
    """Convert a numeric column number (1-based) to its alpha version.
    """
    col_letter = ""
    while col > 0:
        col, remainder = divmod(col - 1, 26)
        col_letter = chr(65 + remainder) + col_letter
    return col_letter


def main():
    """Testing proof of concept for now.
    """
    gc = gspread_client()

    sheet = gc.open("Cloud SQL Docs Auditor").worksheet("404s")
    sheet.clear()
    sheet.append_row(["timestamp", "Documentation Page", "Missing Destination URL"])

    # write values
    for _ in range(4):
        timestamp = datetime.datetime.now().strftime("%D %H:%M:%S")
        sheet.append_row(
            [
                timestamp,
                '=HYPERLINK("https://www.dougmahugh.com/blood-meridian/", "Blood Meridian")',
                '=HYPERLINK("https://www.dougmahugh.com/minimal-vim/", "Minimal Vim")',
            ],
            value_input_option="USER_ENTERED",
        )


if __name__ == "__main__":
    main()
