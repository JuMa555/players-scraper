import sqlite3
import pandas as pd
import re
from rapidfuzz import fuzz

def create_connection(db_name="players.db"):
    conn = sqlite3.connect(db_name)
    return conn

def create_players_table(conn):
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS players (
        player_id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT UNIQUE,
        name TEXT,
        full_name TEXT,
        date_of_birth TEXT,
        age INTEGER,
        place_of_birth TEXT,
        country_of_birth TEXT,
        positions TEXT,
        current_club TEXT,
        national_team TEXT,
        appearances_current_club INTEGER,
        goals_current_club INTEGER,
        scraping_timestamp TEXT
    );
    """)

    conn.commit()

def insert_player_from_csv(conn, row):
    cursor = conn.cursor()

    sql = """
    INSERT OR IGNORE INTO players (
        url, name, full_name, date_of_birth, age,
        place_of_birth, country_of_birth, positions,
        current_club, national_team
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    data = (
        row.get("URL"),
        row.get("Name"),
        row.get("Full name"),
        row.get("Date of birth"),
        int(row["Age"]) if pd.notna(row.get("Age")) else None,
        row.get("City of birth"),
        row.get("Country of birth"),
        row.get("Position"),
        row.get("Current club"),
        row.get("National_team")
    )

    cursor.execute(sql, data)
    conn.commit()


def upsert_player_from_scraper(conn, url, player_data):
    cursor = conn.cursor()

    sql = """
    INSERT INTO players (
        url, name, full_name, date_of_birth, age,
        place_of_birth, country_of_birth, positions,
        current_club, national_team, appearances_current_club,
        goals_current_club, scraping_timestamp
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(url) DO UPDATE SET
        name = excluded.name,
        full_name = excluded.full_name,
        date_of_birth = excluded.date_of_birth,
        age = excluded.age,
        place_of_birth = excluded.place_of_birth,
        country_of_birth = excluded.country_of_birth,
        positions = excluded.positions,
        current_club = excluded.current_club,
        national_team = excluded.national_team,
        appearances_current_club = excluded.appearances_current_club,
        goals_current_club = excluded.goals_current_club,
        scraping_timestamp = excluded.scraping_timestamp;
    """

    values = (
        url,
        player_data.get("name"),
        player_data.get("full_name"),
        player_data.get("date_of_birth"),
        player_data.get("age"),
        player_data.get("place_of_birth"),
        player_data.get("country_of_birth"),
        player_data.get("positions"),
        player_data.get("current_club"),
        player_data.get("national_team"),
        player_data.get("appearances_current_club"),
        player_data.get("goals_current_club"),
        player_data.get("scraping_timestamp")
    )

    cursor.execute(sql, values)
    conn.commit()


def add_columns(conn):
    cursor = conn.cursor()

    cursor.execute("ALTER TABLE players ADD COLUMN AgeCategory TEXT")
    
    cursor.execute("ALTER TABLE players ADD COLUMN GoalsPerClubGame REAL")

    conn.commit()


def enrich_players_data(conn):
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE players
        SET AgeCategory = 
            CASE
                WHEN age IS NULL THEN NULL
                WHEN age <= 23 THEN 'Young'
                WHEN age BETWEEN 24 AND 32 THEN 'MidAge'
                ELSE 'Old'
            END
    """)

    cursor.execute("""
        UPDATE players
        SET GoalsPerClubGame = 
            CASE
                WHEN appearances_current_club IS NULL OR appearances_current_club = 0 THEN NULL
                ELSE CAST(goals_current_club AS REAL) / appearances_current_club
            END
    """)

    conn.commit()

def analyze_club_stats(conn):
    cursor = conn.cursor()

    cursor.execute("""
        SELECT 
            current_club,
            COUNT(*) AS total_players,
            AVG(age) AS avg_age,
            AVG(appearances_current_club) AS avg_appearances
        FROM players
        WHERE current_club IS NOT NULL
        GROUP BY current_club
        ORDER BY total_players DESC
    """)

    results = cursor.fetchall()

    print("\nClub statistics:\n")
    for row in results:
        club, total, avg_age, avg_apps = row
        print(f"{club}: {total} players, average age: {avg_age}, average appearances: {avg_apps}")


def compare_players_by_position(conn, chosen_club):
    cursor = conn.cursor()

    cursor.execute("""
        SELECT 
            p1.name,
            p1.age,
            p1.positions,
            p1.appearances_current_club,
            COUNT(p2.player_id) AS better_players
        FROM players p1
        LEFT JOIN players p2
            ON (
                LOWER(REPLACE(REPLACE(p2.positions, '/', ','), ' ', '')) LIKE '%' || 
                REPLACE(LOWER(REPLACE(REPLACE(p1.positions, '/', ','), ' ', '')), ',', '%') || '%'
                OR
                LOWER(REPLACE(REPLACE(p1.positions, '/', ','), ' ', '')) LIKE '%' || 
                REPLACE(LOWER(REPLACE(REPLACE(p2.positions, '/', ','), ' ', '')), ',', '%') || '%'
            )
            AND p2.age < p1.age
            AND p2.appearances_current_club > p1.appearances_current_club
        WHERE p1.current_club = ?
        GROUP BY p1.name, p1.age, p1.positions, p1.appearances_current_club
        ORDER BY p1.positions, p1.age
    """, (chosen_club,))

    rows = cursor.fetchall()
    print(rows)

    print(f"\nPlayer comparison for club '{chosen_club}':\n")
    for row in rows:
        name, age, position, apps, better_count = row
        apps_text = "no data" if apps is None else f"{apps} appearances"
        print(f"{name} ({position}, {age} years old, {apps_text}) - {better_count} 'better' players")


def normalize_club_name(name):
    if not name:
        return ""
    name = re.sub(r'\s*\(on loan from [^)]+\)', '', name, flags=re.IGNORECASE)
    name = re.sub(r'^(?:[FCA]\.?[C]\.?[ ]*)+\b', '', name, flags=re.IGNORECASE).strip()
    name = re.sub(r'\b[FC\. ]+$', '', name, flags=re.IGNORECASE).strip()
    return name.strip()


def standardize_club_names(conn, threshold=90):
    cursor = conn.cursor()

    cursor.execute("SELECT DISTINCT current_club FROM players WHERE current_club IS NOT NULL")
    original_clubs = [row[0].strip() for row in cursor.fetchall() if row[0]]

    print("\nNormalizing club names...")
    for club in original_clubs:
        normalized = normalize_club_name(club)
        if club != normalized:
            print(f"'{club}' → '{normalized}'")
            cursor.execute(
                "UPDATE players SET current_club = ? WHERE current_club = ?",
                (normalized, club)
            )
    conn.commit()

    cursor.execute("SELECT DISTINCT current_club FROM players WHERE current_club IS NOT NULL")
    clubs = [row[0] for row in cursor.fetchall()]

    standardized = {}
    for club in clubs:
        found = False
        for standard in standardized:
            similarity = fuzz.ratio(club, standard)
            if similarity >= threshold:
                standardized[club] = standardized[standard]
                found = True
                break
        if not found:
            standardized[club] = club

    print("\nFuzzy merging of similar club names...")
    changes = 0
    for old_name, new_name in standardized.items():
        if old_name != new_name:
            print(f"'{old_name}' → '{new_name}'")
            cursor.execute(
                "UPDATE players SET current_club = ? WHERE current_club = ?",
                (new_name, old_name)
            )
            changes += 1

    conn.commit()

    print(f"\nStandardization complete. {changes} names changed.")
