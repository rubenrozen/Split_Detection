"""
Split Detector — détecte les splits et reverse splits sur un portefeuille
Sources : Polygon (tickers "$") + Yahoo Finance direct (autres devises)
Écrit dans "Splits library" de chaque Google Sheet configuré
Envoie un email de notification pour chaque nouvelle ligne écrite
"""

import os
import json
import time
import smtplib
import requests
import gspread
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from google.oauth2.service_account import Credentials


# ── Configuration ────────────────────────────────────────────────────────────

SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

SPREADSHEET_SECRETS = ["PORTFOLIO", "NEXT_HORIZON", "TREND_SPOTTING", "VALUE_UNDERFLOW"]

POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY", "")
GMAIL_ADDRESS   = os.environ.get("GMAIL_ADDRESS", "")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
NOTIFY_EMAIL    = "ruben1rozenfeld@gmail.com"

DETECTION_DAYS  = 7       # fenêtre de détection en jours
POLYGON_SLEEP   = 12      # secondes entre appels Polygon (rate limit 5/min)
SHEET_NAME      = "Splits library"


# ── Google Sheets ─────────────────────────────────────────────────────────────

def get_gspread_client():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS", "")
    if not creds_json:
        raise RuntimeError("Secret GOOGLE_CREDENTIALS manquant.")
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def get_spreadsheet_ids():
    ids = {}
    for name in SPREADSHEET_SECRETS:
        val = os.environ.get(name, "").strip()
        if val:
            ids[name] = val
    return ids


def read_splits_library(sheet):
    """
    Lit A4:G de la feuille Splits library.
    Retourne :
      - tickers_map : { yahoo_ticker: google_ticker }
      - usd_tickers : liste de yahoo tickers avec devise "$"
      - other_tickers : liste de yahoo tickers avec autre devise
      - existing_entries : set de (google_ticker, date_str) déjà écrits
      - next_row : première ligne vide en colonne E
    """
    # Lire A4:G jusqu'à la dernière ligne
    try:
        data = sheet.get("A4:G")
    except Exception as e:
        print(f"  Erreur lecture sheet : {e}")
        return {}, [], [], set(), 4

    tickers_map   = {}   # yahoo → google
    usd_tickers   = []
    other_tickers = []
    existing_entries = set()
    next_row = 4

    for i, row in enumerate(data):
        # Étendre la ligne à 7 colonnes si besoin
        row = list(row) + [""] * (7 - len(row))
        yahoo_ticker  = str(row[0]).strip()   # col A
        google_ticker = str(row[1]).strip()   # col B
        currency      = str(row[3]).strip()   # col D
        existing_e    = str(row[4]).strip()   # col E
        existing_f    = str(row[5]).strip()   # col F (date)

        # Mapping yahoo → google
        if yahoo_ticker and google_ticker:
            tickers_map[yahoo_ticker] = google_ticker

        # Classification par devise
        if yahoo_ticker:
            if currency == "$":
                usd_tickers.append(yahoo_ticker)
            else:
                other_tickers.append(yahoo_ticker)

        # Entrées existantes pour déduplication (col E + col F)
        if existing_e and existing_f:
            existing_entries.add((existing_e, existing_f))

    # Trouver la prochaine ligne libre en col E
    # On relit uniquement col E pour être précis
    try:
        col_e = sheet.col_values(5)  # col E = index 5
        # col_values commence à la ligne 1
        # On cherche la première cellule vide à partir de la ligne 4
        filled = sum(1 for v in col_e if v.strip()) if col_e else 0
        next_row = max(4, filled + 1)
    except Exception:
        next_row = 4

    return tickers_map, usd_tickers, other_tickers, existing_entries, next_row


# ── Polygon ───────────────────────────────────────────────────────────────────

def fetch_splits_polygon(tickers):
    """
    Appelle /v3/reference/splits pour chaque ticker US.
    Retourne une liste de dicts : { yahoo_ticker, date, ratio }
    """
    if not POLYGON_API_KEY:
        print("  ⚠️  POLYGON_API_KEY manquant, skip tickers USD.")
        return []

    date_from = (datetime.today() - timedelta(days=DETECTION_DAYS)).strftime("%Y-%m-%d")
    results = []

    for i, ticker in enumerate(tickers):
        if i > 0:
            time.sleep(POLYGON_SLEEP)

        url = (
            f"https://api.polygon.io/v3/reference/splits"
            f"?ticker={ticker}&execution_date.gte={date_from}"
            f"&limit=10&apiKey={POLYGON_API_KEY}"
        )
        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code != 200:
                print(f"  Polygon HTTP {resp.status_code} pour {ticker}")
                continue
            data = resp.json()
            for split in data.get("results", []):
                exec_date   = split.get("execution_date", "")
                split_from  = split.get("split_from", 1)
                split_to    = split.get("split_to", 1)
                if exec_date and split_from and split_to:
                    results.append({
                        "yahoo_ticker": ticker,
                        "date": exec_date,          # format YYYY-MM-DD
                        "ratio_from": split_from,
                        "ratio_to": split_to,
                    })
                    print(f"  ✅ Polygon — {ticker} : {split_to}:{split_from} le {exec_date}")
        except Exception as e:
            print(f"  Erreur Polygon pour {ticker} : {e}")

    return results


# ── Yahoo Finance direct ───────────────────────────────────────────────────────

def fetch_splits_yahoo(tickers):
    """
    Appelle l'API Yahoo Finance chart pour récupérer events.splits.
    Retourne une liste de dicts : { yahoo_ticker, date, ratio }
    """
    date_from_ts = int((datetime.today() - timedelta(days=DETECTION_DAYS)).timestamp())
    date_to_ts   = int((datetime.today() + timedelta(days=1)).timestamp())
    results = []

    for ticker in tickers:
        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
            f"?period1={date_from_ts}&period2={date_to_ts}"
            f"&interval=1d&events=splits"
        )
        try:
            resp = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code != 200:
                print(f"  Yahoo HTTP {resp.status_code} pour {ticker}")
                continue
            data = resp.json()
            chart_result = (data.get("chart") or {}).get("result") or []
            if not chart_result:
                continue
            events = chart_result[0].get("events") or {}
            splits = events.get("splits") or {}

            for ts_str, split_info in splits.items():
                ts = int(ts_str)
                split_date = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
                numerator   = split_info.get("numerator", 1)
                denominator = split_info.get("denominator", 1)
                results.append({
                    "yahoo_ticker": ticker,
                    "date": split_date,
                    "ratio_from": denominator,
                    "ratio_to": numerator,
                })
                print(f"  ✅ Yahoo — {ticker} : {numerator}:{denominator} le {split_date}")
        except Exception as e:
            print(f"  Erreur Yahoo pour {ticker} : {e}")

    return results


# ── Formatage ──────────────────────────────────────────────────────────────────

def format_date(date_str):
    """Convertit YYYY-MM-DD en DD-MM-YYYY."""
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        return d.strftime("%d-%m-%Y")
    except Exception:
        return date_str


def format_ratio(ratio_to, ratio_from):
    """Formate le ratio en '2:1'."""
    try:
        r_to   = int(ratio_to)
        r_from = int(ratio_from)
        return f"{r_to}:{r_from}"
    except Exception:
        return f"{ratio_to}:{ratio_from}"


# ── Email ──────────────────────────────────────────────────────────────────────

def send_notification(spreadsheet_name, google_ticker, date_str, ratio_str):
    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD:
        print("  ⚠️  Credentials email manquants, notification skippée.")
        return

    subject = f"🔀 Split détecté — {google_ticker} ({date_str})"
    body = (
        f"Un nouveau corporate action a été détecté et enregistré.\n\n"
        f"Portefeuille  : {spreadsheet_name}\n"
        f"Ticker        : {google_ticker}\n"
        f"Date          : {date_str}\n"
        f"Ratio         : {ratio_str}\n\n"
        f"Enregistré automatiquement dans la feuille « Splits library »."
    )

    msg = MIMEMultipart()
    msg["From"]    = GMAIL_ADDRESS
    msg["To"]      = NOTIFY_EMAIL
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    try:
        # Nettoyage du mot de passe (supprime les caractères non-ASCII)
        password = "".join(c for c in GMAIL_APP_PASSWORD.strip() if ord(c) < 128)
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_ADDRESS, password)
            server.sendmail(GMAIL_ADDRESS, NOTIFY_EMAIL, msg.as_string())
        print(f"  📧 Email envoyé pour {google_ticker} ({date_str})")
    except Exception as e:
        print(f"  ⚠️  Erreur envoi email : {e}")


# ── Écriture dans le Sheet ────────────────────────────────────────────────────

def write_splits_to_sheet(sheet, splits, tickers_map, existing_entries, next_row, sheet_label):
    """
    Écrit les nouveaux splits dans le sheet.
    Evite les doublons (google_ticker + date).
    """
    written = 0

    for split in splits:
        yahoo_ticker = split["yahoo_ticker"]
        google_ticker = tickers_map.get(yahoo_ticker)

        if not google_ticker:
            print(f"  ⚠️  Pas de ticker Google trouvé pour {yahoo_ticker}, skip.")
            continue

        date_formatted = format_date(split["date"])
        ratio_str      = format_ratio(split["ratio_to"], split["ratio_from"])
        entry_key      = (google_ticker, date_formatted)

        # Déduplication
        if entry_key in existing_entries:
            print(f"  ⏭️  Doublon ignoré : {google_ticker} le {date_formatted}")
            continue

        # Écriture E, F, G sur la prochaine ligne libre
        try:
            sheet.update(
                range_name=f"E{next_row}:G{next_row}",
                values=[[google_ticker, date_formatted, ratio_str]]
            )
            print(f"  ✍️  Écrit ligne {next_row} : {google_ticker} | {date_formatted} | {ratio_str}")

            # Marquer comme existant pour éviter doublons dans le même run
            existing_entries.add(entry_key)
            next_row += 1
            written  += 1

            # Notification email
            send_notification(sheet_label, google_ticker, date_formatted, ratio_str)

            # Petite pause pour éviter le throttle Sheets
            time.sleep(1)

        except Exception as e:
            print(f"  ❌ Erreur écriture ligne {next_row} : {e}")

    return written


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    today = datetime.today().strftime("%d/%m/%Y")
    print(f"=== Split Detector — {today} ===\n")

    gc  = get_gspread_client()
    ids = get_spreadsheet_ids()

    if not ids:
        print("Aucun spreadsheet configuré dans les secrets.")
        return

    total_written = 0

    for label, spreadsheet_id in ids.items():
        print(f"\n── {label} ({spreadsheet_id[:10]}…) ──")

        try:
            ss    = gc.open_by_key(spreadsheet_id)
            sheet = ss.worksheet(SHEET_NAME)
        except Exception as e:
            print(f"  ❌ Impossible d'ouvrir le sheet : {e}")
            continue

        # Lecture de la feuille
        tickers_map, usd_tickers, other_tickers, existing_entries, next_row = \
            read_splits_library(sheet)

        print(f"  Tickers USD (Polygon) : {len(usd_tickers)}")
        print(f"  Tickers autres (Yahoo) : {len(other_tickers)}")
        print(f"  Entrées existantes     : {len(existing_entries)}")
        print(f"  Prochaine ligne libre  : {next_row}")

        # Détection
        splits = []

        if usd_tickers:
            print(f"\n  [Polygon] Interrogation de {len(usd_tickers)} tickers...")
            splits += fetch_splits_polygon(usd_tickers)

        if other_tickers:
            print(f"\n  [Yahoo] Interrogation de {len(other_tickers)} tickers...")
            splits += fetch_splits_yahoo(other_tickers)

        print(f"\n  Événements détectés : {len(splits)}")

        # Écriture
        if splits:
            written = write_splits_to_sheet(
                sheet, splits, tickers_map,
                existing_entries, next_row, label
            )
            total_written += written
            print(f"  Nouvelles lignes écrites : {written}")
        else:
            print("  Aucun split trouvé pour cette période.")

    print(f"\n=== Terminé — {total_written} ligne(s) écrite(s) au total ===")


if __name__ == "__main__":
    main()
