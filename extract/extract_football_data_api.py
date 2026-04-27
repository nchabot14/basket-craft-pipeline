"""
Extract one season of one competition from football-data.org v4 and land it
into Snowflake RAW_* tables (schema set in bootstrap.sql).

Endpoints pulled (5 total, ~6.5s apart to stay under the free-tier 10 req/min cap):
    /competitions/{code}                              -> RAW_COMPETITIONS  (1 row)
    /competitions/{code}/teams?season={s}             -> RAW_TEAMS         (one row per team)
    /competitions/{code}/matches?season={s}           -> RAW_MATCHES       (one row per match)
    /competitions/{code}/standings?season={s}         -> RAW_STANDINGS     (1 row)
    /competitions/{code}/scorers?season={s}&limit=100 -> RAW_SCORERS       (1 row)

Run:
    .venv/bin/python3 extract/extract_football_data_api.py

Required env vars (.env):
    FOOTBALL_DATA_API_KEY                       free-tier token from football-data.org
    SNOWFLAKE_ACCOUNT / USER / PASSWORD
    SNOWFLAKE_WAREHOUSE / DATABASE / SCHEMA / ROLE

Optional env vars:
    FOOTBALL_DATA_COMPETITION   default: PL
    FOOTBALL_DATA_SEASON        default: 2024
"""
import json
import logging
import os
import sys
import time

import requests
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)
log = logging.getLogger(__name__)

API_BASE = 'https://api.football-data.org/v4'
SOURCE = 'football-data.org'
SLEEP_SECONDS = 6.5
RATE_LIMIT_BACKOFF = 60


def fetch(url, headers):
    """GET url. Retry once on 429 after a 60s sleep. Exit cleanly on 403."""
    for attempt in (1, 2):
        log.info('GET %s (attempt %d)', url, attempt)
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 403:
            log.error(
                '403 Forbidden from %s. The free tier likely does not include this '
                'competition/season combination. Check your plan and FOOTBALL_DATA_* env vars.',
                url,
            )
            sys.exit(1)
        if resp.status_code == 429 and attempt == 1:
            log.warning('429 rate limited; sleeping %ds then retrying once', RATE_LIMIT_BACKOFF)
            time.sleep(RATE_LIMIT_BACKOFF)
            continue
        resp.raise_for_status()
        return resp.json()
    log.error('Failed after retry: %s', url)
    sys.exit(1)


def insert_variant_row(cursor, table, payload, competition_code, season,
                       id_column=None, id_value=None):
    """
    Insert one row whose `payload` column is VARIANT.

    Snowflake can't bind a Python dict to VARIANT directly, so we json.dumps()
    the payload, bind it as STRING, and use PARSE_JSON() server-side. The
    INSERT...SELECT form is required because INSERT...VALUES(PARSE_JSON(%s)...)
    is rejected by the connector.
    """
    payload_json = json.dumps(payload)
    if id_column:
        sql = (
            f'INSERT INTO {table} ({id_column}, payload, source, competition_code, season) '
            f'SELECT %s, PARSE_JSON(%s), %s, %s, %s'
        )
        cursor.execute(sql, (id_value, payload_json, SOURCE, competition_code, season))
    else:
        sql = (
            f'INSERT INTO {table} (payload, source, competition_code, season) '
            f'SELECT PARSE_JSON(%s), %s, %s, %s'
        )
        cursor.execute(sql, (payload_json, SOURCE, competition_code, season))


def main():
    api_key = os.environ['FOOTBALL_DATA_API_KEY']
    code = os.environ.get('FOOTBALL_DATA_COMPETITION', 'PL')
    season = os.environ.get('FOOTBALL_DATA_SEASON', '2024')
    headers = {'X-Auth-Token': api_key}

    log.info('Extracting competition=%s season=%s', code, season)

    competition = fetch(f'{API_BASE}/competitions/{code}', headers)
    time.sleep(SLEEP_SECONDS)
    teams = fetch(f'{API_BASE}/competitions/{code}/teams?season={season}', headers)
    time.sleep(SLEEP_SECONDS)
    matches = fetch(f'{API_BASE}/competitions/{code}/matches?season={season}', headers)
    time.sleep(SLEEP_SECONDS)
    standings = fetch(f'{API_BASE}/competitions/{code}/standings?season={season}', headers)
    time.sleep(SLEEP_SECONDS)
    scorers = fetch(f'{API_BASE}/competitions/{code}/scorers?season={season}&limit=100', headers)

    log.info('Connecting to Snowflake')
    conn = snowflake.connector.connect(
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
        database=os.environ['SNOWFLAKE_DATABASE'],
        schema=os.environ['SNOWFLAKE_SCHEMA'],
        role=os.environ['SNOWFLAKE_ROLE'],
    )

    counts = {}
    try:
        cur = conn.cursor()
        try:
            insert_variant_row(cur, 'RAW_COMPETITIONS', competition, code, season)
            counts['RAW_COMPETITIONS'] = 1
            log.info('Loaded RAW_COMPETITIONS (1 row)')

            team_list = teams.get('teams', [])
            for team in team_list:
                insert_variant_row(
                    cur, 'RAW_TEAMS', team, code, season,
                    id_column='team_id', id_value=team.get('id'),
                )
            counts['RAW_TEAMS'] = len(team_list)
            log.info('Loaded RAW_TEAMS (%d rows)', len(team_list))

            match_list = matches.get('matches', [])
            for match in match_list:
                insert_variant_row(
                    cur, 'RAW_MATCHES', match, code, season,
                    id_column='match_id', id_value=match.get('id'),
                )
            counts['RAW_MATCHES'] = len(match_list)
            log.info('Loaded RAW_MATCHES (%d rows)', len(match_list))

            insert_variant_row(cur, 'RAW_STANDINGS', standings, code, season)
            counts['RAW_STANDINGS'] = 1
            log.info('Loaded RAW_STANDINGS (1 row)')

            insert_variant_row(cur, 'RAW_SCORERS', scorers, code, season)
            counts['RAW_SCORERS'] = 1
            log.info('Loaded RAW_SCORERS (1 row)')

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
    finally:
        conn.close()

    print()
    print('Row count summary:')
    for table, n in counts.items():
        print(f'  {table:<20} {n:>6}')
    print(f'  {"TOTAL":<20} {sum(counts.values()):>6}')


if __name__ == '__main__':
    main()
