import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import re
from db_utils import create_connection, create_players_table, insert_player_from_csv, upsert_player_from_scraper, add_columns, enrich_players_data, analyze_club_stats, compare_players_by_position, normalize_club_name, standardize_club_names

def main():
    conn = create_connection()
    create_players_table(conn)

    df = pd.read_csv("data/playersData.csv", sep=";", encoding="utf-8")

    for _, row in df.iterrows():
        insert_player_from_csv(conn, row)

    conn.close()
    print("CSV loading completed.")


def scrape_single_player(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"Error {response.status_code} fetching page: {url}")
        return

    soup = BeautifulSoup(response.content, "html.parser")

    player_data = parse_player_data(soup)
    for key, value in player_data.items():
        print(f"{key}: {value}")

    conn = create_connection()
    upsert_player_from_scraper(conn, url, player_data)
    conn.close()


def parse_player_data(soup):
    data = {
        "name": None,
        "full_name": None,
        "date_of_birth": None,
        "age": None,
        "place_of_birth": None,
        "country_of_birth": None,
        "positions": None,
        "current_club": None,
        "national_team": None,
        "appearances_current_club": None,
        "goals_current_club": None,
        "scraping_timestamp": datetime.now().isoformat()
    }

    name_tag = soup.find("h1", {"id": "firstHeading"})
    if name_tag:
        data["name"] = name_tag.text.strip()

    infobox = soup.find("table", {"class": "infobox"})
    if not infobox:
        return data

    rows = infobox.find_all("tr")

    for row in rows:
        header = row.find("th")
        cell = row.find("td")
        if not header or not cell:
            continue

        label = header.text.strip().lower()

        if "full name" in label:
            data["full_name"] = cell.text.strip().split("[")[0]

        elif "date of birth" in label:
            dob_text = cell.text.strip()
            dob_text = re.sub(r"\[.*?\]", "", dob_text)

            try:
                age_match = re.search(r"age\s*(\d+)", dob_text, re.IGNORECASE)
                if age_match:
                    data["age"] = int(age_match.group(1))
            except (IndexError, ValueError):
                pass

            try:
                match = re.search(r"\((\d{4}-\d{2}-\d{2})\)", dob_text)
                if match:
                    iso_date = match.group(1)
                    date_obj = datetime.strptime(iso_date, "%Y-%m-%d")
                    data["date_of_birth"] = date_obj.strftime("%d.%m.%Y")
                else:
                    dob_clean = dob_text.split("(")[0].strip()
                    date_obj = datetime.strptime(dob_clean, "%d %B %Y")
                    data["date_of_birth"] = date_obj.strftime("%d.%m.%Y")
            except Exception as e:
                print("Error parsing date:", e)
                data["date_of_birth"] = None

        elif "place of birth" in label:
            place = cell.text.strip().split("[")[0]
            data["place_of_birth"] = place
            if "," in place:
                data["country_of_birth"] = place.split(",")[-1].strip()
            else:
                data["country_of_birth"] = place.strip()

        elif "position" in label:
            data["positions"] = cell.text.strip().split("[")[0]

        elif "current team" in label:
            data["current_club"] = cell.text.strip().split("[")[0]

        elif "national team" in label:
            data["national_team"] = cell.text.strip().split("[")[0]

    in_senior = False
    last_apps = None
    last_goals = None

    for row in rows:
        th = row.find("th")
        if th:
            text = th.get_text(strip=True).lower()
            if "senior career" in text:
                in_senior = True
                continue
            if in_senior and ("international career" in text):
                break

        if not in_senior:
            continue

        tds = row.find_all("td")
        if len(tds) >= 3:
            apps_text = tds[-2].get_text(strip=True)
            goals_text = tds[-1].get_text(strip=True)
            if apps_text.isdigit():
                last_apps = int(apps_text)
            match_goals = re.search(r"\d+", goals_text)
            if match_goals:
                last_goals = int(match_goals.group())

    if last_apps is not None:
        data["appearances_current_club"] = last_apps
    if last_goals is not None:
        data["goals_current_club"] = last_goals

    in_international = False
    for row in rows:
        th = row.find("th")
        if th:
            text = th.get_text(strip=True).lower()
            if "international career" in text or "national team" in text:
                in_international = True
                continue

        if not in_international:
            continue

        tds = row.find_all("td")
        if len(tds) >= 2:
            team = tds[1].get_text(strip=True)
            team = re.sub(r"^\d{4}(–\d{4})?\s*–?\s*", "", team).strip()

            if not re.search(r"u-?\d+", team.lower()):
                data["national_team"] = team

    return data


def scrape_all_players(csv_path="data/playersURLs.csv"):

    urls_df = pd.read_csv(csv_path, header=None, names=["URL"], encoding="utf-8")

    total = len(urls_df)
    print(f"\nScraping {total} players...")

    for i, row in urls_df.iterrows():
        url = row["URL"]
        print(f"\n[{i+1}/{total}] Scraping: {url}")

        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            }

            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                print(f"Error {response.status_code} fetching page: {url}")
                continue

            soup = BeautifulSoup(response.content, "html.parser")
            player_data = parse_player_data(soup)

            conn = create_connection()
            upsert_player_from_scraper(conn, url, player_data)
            conn.close()

            print("Saved")

            time.sleep(1)

        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":

    main()
    scrape_all_players() 

    conn = create_connection()
    
    add_columns(conn)
    enrich_players_data(conn)
    
    standardize_club_names(conn)
    analyze_club_stats(conn)
    compare_players_by_position(conn, "Barcelona")
    
    conn.close()
