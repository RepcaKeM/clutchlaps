import os
import json
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from datetime import datetime
import logging
import shutil

def parse_telemetry_value(value_str):
    """Parses a telemetry value string (e.g., '61.843 s') into a float."""
    if not isinstance(value_str, str) or not value_str.strip():
        return None
    try:
        # Remove units and whitespace, then convert to float
        numeric_part = value_str.replace(' s', '').replace(' m', '').replace(' km/h', '').strip()
        return float(numeric_part)
    except ValueError:
        logging.warning(f"Could not parse telemetry value: {value_str}")
        return None


# Configure logging
log_file_path = f'/app/logs/data_transformer/data_transformer_logs_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(log_file_path, mode='a'),
                        logging.StreamHandler() # Also log to console
                    ])

# Database connection parameters from environment variables
DB_HOST = os.environ.get('POSTGRES_HOST', 'db')
DB_NAME = os.environ.get('POSTGRES_DB', 'speedway_db')
DB_USER = os.environ.get('POSTGRES_USER', 'speedway_user')
DB_PASSWORD = os.environ.get('PGPASSWORD', 'speedgres_password')

OUTPUT_DIR = 'output' # Relative to the script's location if it's inside speedway_scraper

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        logging.info("Successfully connected to the database.")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Error connecting to PostgreSQL Database: {e}")
        raise

def get_or_create_id(cursor, table_name, column_name, value, returning_id_column="id"):
    """
    Generic function to get an ID if a value exists, or create a new row and return its ID.
    Assumes the table has a serial primary key named as per returning_id_column.
    Assumes the column to check for uniqueness is specified by column_name.
    """
    logging.info(f"get_or_create_id called for table: {table_name}, column: {column_name}, value type: {type(value)}, value: {value}")
    if value is None or (isinstance(value, str) and not value.strip()):
        return None

    query_select = sql.SQL("SELECT {} FROM {} WHERE {} = %s").format(
        sql.Identifier(returning_id_column),
        sql.Identifier(table_name),
        sql.Identifier(column_name)
    )
    cursor.execute(query_select, (value,))
    result = cursor.fetchone()

    if result:
        return result[0]
    else:
        query_insert = sql.SQL("INSERT INTO {} ({}) VALUES (%s) ON CONFLICT ({}) DO NOTHING RETURNING {}").format(
            sql.Identifier(table_name),
            sql.Identifier(column_name),
            sql.Identifier(column_name), # Conflict target
            sql.Identifier(returning_id_column)
        )
        try:
            cursor.execute(query_insert, (value,))
            # If insert was successful, fetch the ID
            inserted_id = cursor.fetchone()
            if inserted_id:
                return inserted_id[0]
            else:
                 # If ON CONFLICT DO NOTHING was triggered, the RETURNING clause won't return anything.
                 # Select the existing ID.
                 cursor.execute(query_select, (value,))
                 result = cursor.fetchone()
                 if result:
                     return result[0]
                 else:
                     logging.error(f"Failed to get or create ID for {table_name} with value '{value}' after conflict.")
                     return None # Should not happen if ON CONFLICT DO NOTHING works as expected
        except psycopg2.Error as e:
            logging.error(f"Error during get_or_create_id insert/select for {table_name} with value '{value}': {e}")
            return None


def parse_score(score_raw):
    """Parses a raw score string into numeric points, bonus flag, and accident text."""
    points_numeric = 0
    with_bonus = False
    accident_text = None

    if not score_raw or score_raw.strip() == "":
        return points_numeric, with_bonus, accident_text

    score_str = score_raw.strip().upper()

    if "'" in score_str:
        with_bonus = True
        score_str = score_str.replace("'", "")

    try:
        points_numeric = int(score_str)
    except ValueError:
        # Handle non-numeric scores (accident codes)
        accident_text = score_str
        points_numeric = 0 # Assume 0 points for accident codes

    return points_numeric, with_bonus, accident_text

def transform_and_load(conn):
    """Reads JSON files, transforms data, and loads into the database."""
    logging.info("Starting data transformation and loading process.")
    cursor = conn.cursor()
    # Input directory for raw JSON files (mounted from ./output/ekstraliga_scraper)
    input_dir = '/app/ekstraligapl/output'
    # Output directory for processed JSON files (mounted from ./output/data_transformer)
    processed_dir = '/app/output/data_transformer'

    if not os.path.exists(input_dir):
        logging.error(f"Input directory not found: {input_dir}")
        return

    # Create the processed directory if it doesn't exist
    if not os.path.exists(processed_dir):
        try:
            os.makedirs(processed_dir)
            logging.info(f"Created processed directory: {processed_dir}")
        except OSError as e:
            logging.error(f"Error creating processed directory {processed_dir}: {e}")
            return # Cannot proceed if processed directory cannot be created


    json_files = [f for f in os.listdir(input_dir) if f.endswith('.json')]
    logging.info(f"Found {len(json_files)} JSON files in {input_dir}")

    for filename in json_files:
        filepath = os.path.join(input_dir, filename) # Corrected path
        logging.info(f"Processing file: {filename}")
        logging.info(f"Attempting to open file: {filepath}")

        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                logging.info(f"Successfully opened file: {filepath}")
                match_data = json.load(f)
                logging.info(f"Successfully loaded JSON data from file: {filepath}")

            # Start a transaction for this file
            conn.autocommit = False

            # --- Process Lookup Tables First ---

            # Arena
            arena_name = match_data.get('arena')
            arena_id = get_or_create_id(cursor, 'arenas', 'name', arena_name, 'arena_id')

            # Competitions
            competition_name = match_data.get('competition')
            competition_id = get_or_create_id(cursor, 'competitions', 'name', competition_name, 'competition_id')

            # Referees
            referee_name = match_data.get('referee')
            referee_id = get_or_create_id(cursor, 'referees', 'name', referee_name, 'referee_id')

            # Track Commissioners
            commissioner_name = match_data.get('track_commissioner')
            commissioner_id = get_or_create_id(cursor, 'track_commissioners', 'name', commissioner_name, 'commissioner_id')

            # Teams
            home_team_full_name = match_data.get('team1', {}).get('team_name')
            home_team_code = match_data.get('home_team_details') # Use abbreviation as team_code
            away_team_full_name = match_data.get('team2', {}).get('team_name')
            away_team_code = match_data.get('away_team_details') # Use abbreviation as team_code

            # Process Home Team
            home_team_id = None
            if home_team_code:
                # Try to find by team_code first
                query_select_code = sql.SQL("SELECT team_id FROM teams WHERE team_code = %s").format()
                cursor.execute(query_select_code, (home_team_code,))
                result = cursor.fetchone()
                if result:
                    home_team_id = result[0]
                    # Update existing team with full_name and arena_id if null
                    update_query = sql.SQL("""
                        UPDATE teams
                        SET full_name = COALESCE(full_name, %s),
                            arena_id = COALESCE(arena_id, %s)
                        WHERE team_id = %s
                    """)
                    cursor.execute(update_query, (home_team_full_name, arena_id, home_team_id))
                    logging.info(f"Found home team by code '{home_team_code}', updated ID: {home_team_id}")

            if home_team_id is None and home_team_full_name:
                # If not found by code, try to find by full_name
                query_select_name = sql.SQL("SELECT team_id FROM teams WHERE full_name = %s").format()
                cursor.execute(query_select_name, (home_team_full_name,))
                result = cursor.fetchone()
                if result:
                    home_team_id = result[0]
                    # Update existing team with team_code and arena_id if null
                    update_query = sql.SQL("""
                        UPDATE teams
                        SET team_code = COALESCE(team_code, %s),
                            arena_id = COALESCE(arena_id, %s)
                        WHERE team_id = %s
                    """)
                    cursor.execute(update_query, (home_team_code, arena_id, home_team_id))
                    logging.info(f"Found home team by name '{home_team_full_name}', updated ID: {home_team_id}")

            if home_team_id is None:
                # If not found by either, insert new team
                if home_team_code or home_team_full_name: # Only insert if we have at least one identifier
                    insert_query = sql.SQL("""
                        INSERT INTO teams (team_code, full_name, arena_id)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (team_code) DO NOTHING -- Safeguard
                        RETURNING team_id
                    """)
                    cursor.execute(insert_query, (home_team_code, home_team_full_name, arena_id))
                    inserted_id = cursor.fetchone()
                    if inserted_id:
                        home_team_id = inserted_id[0]
                        logging.info(f"Inserted new home team, ID: {home_team_id}")
                    else:
                         # Conflict occurred, retrieve the existing ID by code
                         if home_team_code:
                             query_select_code = sql.SQL("SELECT team_id FROM teams WHERE team_code = %s").format()
                             cursor.execute(query_select_code, (home_team_code,))
                             result = cursor.fetchone()
                             if result:
                                 home_team_id = result[0]
                                 logging.info(f"Conflict on home team insert, retrieved existing ID by code: {home_team_id}")
                             else:
                                 logging.error(f"Failed to get or create home team with code '{home_team_code}' and name '{home_team_full_name}' after conflict.")
                         else:
                             logging.error(f"Failed to get or create home team with name '{home_team_full_name}' (no code provided).")
                else:
                    logging.warning(f"Cannot process home team: neither code nor name provided in file {filename}.")


            # Process Away Team
            away_team_id = None
            if away_team_code:
                # Try to find by team_code first
                query_select_code = sql.SQL("SELECT team_id FROM teams WHERE team_code = %s").format()
                cursor.execute(query_select_code, (away_team_code,))
                result = cursor.fetchone()
                if result:
                    away_team_id = result[0]
                    # Update existing team with full_name if null
                    update_query = sql.SQL("""
                        UPDATE teams
                        SET full_name = COALESCE(full_name, %s)
                        WHERE team_id = %s
                    """)
                    cursor.execute(update_query, (away_team_full_name, away_team_id))
                    logging.info(f"Found away team by code '{away_team_code}', updated ID: {away_team_id}")

            if away_team_id is None and away_team_full_name:
                # If not found by code, try to find by full_name
                query_select_name = sql.SQL("SELECT team_id FROM teams WHERE full_name = %s").format()
                cursor.execute(query_select_name, (away_team_full_name,))
                result = cursor.fetchone()
                if result:
                    away_team_id = result[0]
                    # Update existing team with team_code if null
                    update_query = sql.SQL("""
                        UPDATE teams
                        SET team_code = COALESCE(team_code, %s)
                        WHERE team_id = %s
                    """)
                    cursor.execute(update_query, (away_team_code, away_team_id))
                    logging.info(f"Found away team by name '{away_team_full_name}', updated ID: {away_team_id}")

            if away_team_id is None:
                 # If not found by either, insert new team
                 if away_team_code or away_team_full_name: # Only insert if we have at least one identifier
                     insert_query = sql.SQL("""
                         INSERT INTO teams (team_code, full_name)
                         VALUES (%s, %s)
                         ON CONFLICT (team_code) DO NOTHING -- Safeguard
                         RETURNING team_id
                     """)
                     cursor.execute(insert_query, (away_team_code, away_team_full_name))
                     inserted_id = cursor.fetchone()
                     if inserted_id:
                         away_team_id = inserted_id[0]
                         logging.info(f"Inserted new away team, ID: {away_team_id}")
                     else:
                          # Conflict occurred, retrieve the existing ID by code
                          if away_team_code:
                              query_select_code = sql.SQL("SELECT team_id FROM teams WHERE team_code = %s").format()
                              cursor.execute(query_select_code, (away_team_code,))
                              result = cursor.fetchone()
                              if result:
                                  away_team_id = result[0]
                                  logging.info(f"Conflict on away team insert, retrieved existing ID by code: {away_team_id}")
                              else:
                                  logging.error(f"Failed to get or create away team with code '{away_team_code}' and name '{away_team_full_name}' after conflict.")
                          else:
                              logging.error(f"Failed to get or create away team with name '{away_team_full_name}' (no code provided).")
                 else:
                     logging.warning(f"Cannot process away team: neither code nor name provided in file {filename}.")


            # Riders (collect all unique riders from both teams)
            all_riders_in_match = []
            if match_data.get('team1', {}).get('riders'):
                all_riders_in_match.extend(match_data['team1']['riders'])
            if match_data.get('team2', {}).get('riders'):
                all_riders_in_match.extend(match_data['team2']['riders'])

            rider_name_to_id = {}
            for rider_data in all_riders_in_match:
                rider_name = rider_data.get('name')
                if rider_name and rider_name.strip():
                    rider_id = get_or_create_id(cursor, 'riders', 'name', rider_name, 'rider_id')
                    if rider_id:
                        rider_name_to_id[rider_name] = rider_id

            # --- Process Match Details ---

            # Match
            match_datetime_str = match_data.get('match_date')
            match_datetime = None
            if match_datetime_str:
                try:
                    # Assuming format "DD.MM.YYYY HH:MM"
                    match_datetime = datetime.strptime(match_datetime_str, '%d.%m.%Y %H:%M')
                except ValueError:
                    logging.warning(f"Could not parse match_date: {match_datetime_str} in file {filename}")

            attendance = None
            attendance_summary = match_data.get('attendance_summary')
            if attendance_summary and attendance_summary.isdigit():
                 attendance = int(attendance_summary)

            insert_match_query = sql.SQL("""
                INSERT INTO matches (
                    match_url, source_system, competition_id, round_type, round_name,
                    match_datetime, attendance, referee_id, track_commissioner_id, arena_id,
                    home_team_id, away_team_id, home_score, away_score, telemetry_data_status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (match_url) DO UPDATE SET
                    source_system = EXCLUDED.source_system,
                    competition_id = EXCLUDED.competition_id,
                    round_type = EXCLUDED.round_type,
                    round_name = EXCLUDED.round_name,
                    match_datetime = EXCLUDED.match_datetime,
                    attendance = EXCLUDED.attendance,
                    referee_id = EXCLUDED.referee_id,
                    track_commissioner_id = EXCLUDED.track_commissioner_id,
                    arena_id = EXCLUDED.arena_id,
                    home_team_id = EXCLUDED.home_team_id,
                    away_team_id = EXCLUDED.away_team_id,
                    home_score = EXCLUDED.home_score,
                    away_score = EXCLUDED.away_score,
                    telemetry_data_status = EXCLUDED.telemetry_data_status
                RETURNING match_id
            """)
            cursor.execute(insert_match_query, (
                match_data.get('match_url'),
                match_data.get('source'),
                competition_id,
                match_data.get('round_type'),
                match_data.get('round'),
                match_datetime,
                attendance,
                referee_id,
                commissioner_id,
                arena_id,
                home_team_id,
                away_team_id,
                int(match_data.get('home_score_details', 0)) if match_data.get('home_score_details', '').isdigit() else 0,
                int(match_data.get('away_score_details', 0)) if match_data.get('away_score_details', '').isdigit() else 0,
                json.dumps(match_data.get('telemetry_data')) if isinstance(match_data.get('telemetry_data'), (dict, list)) else match_data.get('telemetry_data') # Convert dict or list to JSON string
            ))
            match_id = cursor.fetchone()[0]

            # --- Process Match-Specific Details ---

            # Match Team Info
            match_team_info_data = []
            if match_data.get('team1'):
                team1_data = match_data['team1']
                match_team_info_data.append((
                    match_id,
                    home_team_id,
                    team1_data.get('team_name'),
                    team1_data.get('manager'),
                    team1_data.get('coach'),
                    team1_data.get('head_of_team')
                ))
            if match_data.get('team2'):
                team2_data = match_data['team2']
                match_team_info_data.append((
                    match_id,
                    away_team_id,
                    team2_data.get('team_name'),
                    team2_data.get('manager'),
                    team2_data.get('coach'),
                    team2_data.get('head_of_team')
                ))

            if match_team_info_data:
                insert_match_team_info_query = sql.SQL("""
                    INSERT INTO match_team_info (match_id, team_id, match_specific_team_name, manager_name, coach_name, head_of_team_name)
                    VALUES %s
                    ON CONFLICT (match_id, team_id) DO UPDATE SET
                        match_specific_team_name = EXCLUDED.match_specific_team_name,
                        manager_name = EXCLUDED.manager_name,
                        coach_name = EXCLUDED.coach_name,
                        head_of_team_name = EXCLUDED.head_of_team_name
                """)
                execute_values(cursor, insert_match_team_info_query, match_team_info_data)

            # Match Rider Stats
            match_rider_stats_data = []
            rider_match_stat_id_map = {} # Map (match_id, rider_id) to match_rider_stat_id

            def process_team_rider_stats(team_data, team_id):
                if team_data and team_data.get('riders'):
                    for rider_data in team_data['riders']:
                        logging.info(f"Processing rider data for match_rider_stats: {rider_data}")
                        rider_name = rider_data.get('name')
                        rider_id = rider_name_to_id.get(rider_name)

                        if rider_id:
                            rider_number = rider_data.get('number')
                            scores_raw = rider_data.get('scores') # Get scores, don't default to [] yet
                            logging.info(f"Rider {rider_name} ({rider_id}): scores_raw type: {type(scores_raw)}, content: {scores_raw}")

                            # Explicitly check for dict and handle other unexpected types
                            if isinstance(scores_raw, dict):
                                logging.error(f"Expected list or None for scores_raw for rider {rider_name} ({rider_id}) in file {filename}, but got dict. Skipping scores for this rider.")
                                scores_raw = [] # Default to empty list if it's a dict
                            elif not isinstance(scores_raw, list):
                                logging.warning(f"Expected list or None for scores_raw for rider {rider_name} ({rider_id}) in file {filename}, but got {type(scores_raw)}. Using empty list.")
                                scores_raw = [] # Default to empty list for other unexpected types
                            # If scores_raw is None or already a list, it proceeds as is

                            total_sum = int(rider_data.get('sum', 0)) if rider_data.get('sum', '').isdigit() else None # Use None for empty/non-digit
                            bonus_sum = int(rider_data.get('bonus', 0)) if rider_data.get('bonus', '').isdigit() else None # Use None for empty/non-digit

                            match_rider_stats_data.append((
                                match_id, team_id, rider_id, rider_number,
                                total_sum, bonus_sum, scores_raw
                            ))

            process_team_rider_stats(match_data.get('team1'), home_team_id)
            process_team_rider_stats(match_data.get('team2'), away_team_id)

            if match_rider_stats_data:
                logging.info(f"Preparing to insert into match_rider_stats. Data sample: {match_rider_stats_data[:5]}") # Log sample data
                insert_match_rider_stats_query = sql.SQL("""
                    INSERT INTO match_rider_stats (match_id, team_id, rider_id, rider_number_in_match, total_sum_points, total_bonus_points, raw_scores_array)
                    VALUES %s
                    ON CONFLICT (match_id, team_id, rider_id) DO UPDATE SET
                        rider_number_in_match = EXCLUDED.rider_number_in_match,
                        total_sum_points = EXCLUDED.total_sum_points,
                        total_bonus_points = EXCLUDED.total_bonus_points,
                        raw_scores_array = EXCLUDED.raw_scores_array
                    RETURNING match_rider_stat_id, match_id, rider_id
                """)
                # Use execute_values with returning to get the generated IDs
                results = execute_values(cursor, insert_match_rider_stats_query, match_rider_stats_data, fetch=True)
                for stat_id, mid, rid in results:
                    rider_match_stat_id_map[(mid, rid)] = stat_id


            # Heats and Heat Participants
            if match_data.get('match_details'):
                heat_sequence_counter = 0
                for heat_data in match_data['match_details']:
                    logging.info(f"Processing heat data: {heat_data}")
                    heat_sequence_counter += 1 # Increment sequence for each heat object

                    heat_display_number = heat_data.get('heat_number')
                    if not heat_display_number:
                        logging.warning(f"Missing heat_number in heat data for file {filename}. Skipping heat.")
                        continue

                    # Insert Heat
                    insert_heat_query = sql.SQL("""
                        INSERT INTO heats (match_id, heat_display_number, heat_sequence_in_match, hometeam_heat_score, awayteam_heat_score, hometeam_current_match_score_after_heat, awayteam_current_match_score_after_heat)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (match_id, heat_sequence_in_match) DO UPDATE SET
                            heat_display_number = EXCLUDED.heat_display_number,
                            hometeam_heat_score = EXCLUDED.hometeam_heat_score,
                            awayteam_heat_score = EXCLUDED.awayteam_heat_score,
                            hometeam_current_match_score_after_heat = EXCLUDED.hometeam_current_match_score_after_heat,
                            awayteam_current_match_score_after_heat = EXCLUDED.awayteam_current_match_score_after_heat
                        RETURNING heat_id
                    """)
                    cursor.execute(insert_heat_query, (
                        match_id,
                        heat_display_number,
                        heat_sequence_counter,
                        int(heat_data.get('hometeam_heat_score', 0)) if heat_data.get('hometeam_heat_score', '').isdigit() else None, # Use None for empty/non-digit
                        int(heat_data.get('awayteam_heat_score', 0)) if heat_data.get('awayteam_heat_score', '').isdigit() else None, # Use None for empty/non-digit
                        int(heat_data.get('hometeam_current_match_score', 0)) if heat_data.get('hometeam_current_match_score', '').isdigit() else None, # Use None for empty/non-digit
                        int(heat_data.get('awayteam_current_match_score', 0)) if heat_data.get('awayteam_current_match_score', '').isdigit() else None, # Use None for empty/non-digit
                    ))
                    heat_id = cursor.fetchone()[0]

                    # Heat Participants
                    heat_participants_data = []
                    if heat_data.get('riders'):
                        # Get all telemetry data for the match, if available
                        # Assuming telemetry_data in the JSON is a list of rider summaries
                        all_rider_telemetry_list = match_data.get('telemetry_data', [])
                        if not isinstance(all_rider_telemetry_list, list):
                             logging.warning(f"Expected list for telemetry_data in file {filename}, but got {type(all_rider_telemetry_list)}. Skipping telemetry for this file.")
                             all_rider_telemetry_list = [] # Ensure it's a list

                        logging.info(f"All rider telemetry data list for match: {all_rider_telemetry_list}")

                        for participant_data in heat_data['riders']:
                            logging.info(f"Processing heat participant data: {participant_data}")
                            rider_name = participant_data.get('rider')
                            rider_id = rider_name_to_id.get(rider_name)

                            if rider_id:
                                # Get the match_rider_stat_id for this rider in this match
                                match_rider_stat_id = rider_match_stat_id_map.get((match_id, rider_id))

                                if match_rider_stat_id is None:
                                     logging.warning(f"Could not find match_rider_stat_id for rider_id {rider_id} ({rider_name}) in match_id {match_id}. Skipping heat participant.")
                                     continue # Skip this participant if match_rider_stat_id cannot be determined

                                score_raw = participant_data.get('rider_score', '')
                                points_numeric, with_bonus, accident_text = parse_score(score_raw)
                                received_warning = bool(participant_data.get('warning')) # True if warning is not None/empty

                                # --- Extract and parse telemetry data ---
                                participant_telemetry_details = None
                                # Find the rider's overall telemetry summary
                                rider_telemetry_summary = None
                                # Match rider by name (case-insensitive for robustness)
                                rider_name_upper = rider_name.upper() if rider_name else None
                                for rider_tele_summary in all_rider_telemetry_list:
                                    if rider_tele_summary.get('rider_name', '').upper() == rider_name_upper:
                                        rider_telemetry_summary = rider_tele_summary
                                        break

                                if rider_telemetry_summary and isinstance(rider_telemetry_summary.get('detailed_telemetry'), list):
                                    # Find the specific heat telemetry within the rider's detailed_telemetry list
                                    for heat_tele_detail in rider_telemetry_summary['detailed_telemetry']:
                                        # Match by heat number (handle potential trailing dot in heat_number)
                                        if heat_tele_detail.get('heat_number', '').replace('.', '') == heat_display_number.replace('.', ''):
                                            participant_telemetry_details = heat_tele_detail
                                            break
                                elif rider_telemetry_summary:
                                     logging.warning(f"Expected list for detailed_telemetry for rider {rider_name} in file {filename}, but got {type(rider_telemetry_summary.get('detailed_telemetry'))}. Skipping detailed telemetry for this rider.")


                                lap_time_seconds = None
                                distance_meters = None
                                vmax_kmh = None
                                lap1_time_seconds = None
                                lap2_time_seconds = None
                                lap3_time_seconds = None
                                lap4_time_seconds = None

                                if participant_telemetry_details:
                                    lap_time_seconds = parse_telemetry_value(participant_telemetry_details.get('lap_time'))
                                    distance_meters = parse_telemetry_value(participant_telemetry_details.get('distance'))
                                    vmax_kmh = parse_telemetry_value(participant_telemetry_details.get('vmax_lap'))
                                    lap1_time_seconds = parse_telemetry_value(participant_telemetry_details.get('lap1_time'))
                                    lap2_time_seconds = parse_telemetry_value(participant_telemetry_details.get('lap2_time'))
                                    lap3_time_seconds = parse_telemetry_value(participant_telemetry_details.get('lap3_time'))
                                    lap4_time_seconds = parse_telemetry_value(participant_telemetry_details.get('lap4_time'))
                                # --- End telemetry extraction ---


                                heat_participants_data.append((
                                    heat_id,
                                    match_rider_stat_id,
                                    rider_name, # Store rider_name as it appeared in heat data
                                    participant_data.get('substituted_rider'),
                                    # Check if starting_field is empty or None, use None if it is
                                    participant_data.get('starting_field') if participant_data.get('starting_field') and participant_data.get('starting_field').strip() else None,
                                    participant_data.get('helmet_color'),
                                    score_raw,
                                    points_numeric,
                                    with_bonus,
                                    accident_text,
                                    received_warning,
                                    # Add new telemetry values
                                    lap_time_seconds,
                                    distance_meters,
                                    vmax_kmh,
                                    lap1_time_seconds,
                                    lap2_time_seconds,
                                    lap3_time_seconds,
                                    lap4_time_seconds
                                ))

                    if heat_participants_data:
                        logging.info(f"Preparing to insert into heat_participants. Data sample: {heat_participants_data[:5]}") # Log sample data
                        insert_heat_participants_query = sql.SQL("""
                            INSERT INTO heat_participants (
                                heat_id, match_rider_stat_id, rider_name, substituted_rider_name,
                                starting_gate, helmet_color, score_raw, score, with_bonus, accident, with_warning,
                                lap_time_seconds, distance_meters, vmax_kmh, lap1_time_seconds, lap2_time_seconds, lap3_time_seconds, lap4_time_seconds
                            ) VALUES %s
                            ON CONFLICT (heat_id, match_rider_stat_id) DO UPDATE SET
                                rider_name = EXCLUDED.rider_name,
                                substituted_rider_name = EXCLUDED.substituted_rider_name,
                                starting_gate = EXCLUDED.starting_gate,
                                helmet_color = EXCLUDED.helmet_color,
                                score_raw = EXCLUDED.score_raw,
                                score = EXCLUDED.score,
                                with_bonus = EXCLUDED.with_bonus,
                                accident = EXCLUDED.accident,
                                with_warning = EXCLUDED.with_warning,
                                lap_time_seconds = EXCLUDED.lap_time_seconds,
                                distance_meters = EXCLUDED.distance_meters,
                                vmax_kmh = EXCLUDED.vmax_kmh,
                                lap1_time_seconds = EXCLUDED.lap1_time_seconds,
                                lap2_time_seconds = EXCLUDED.lap2_time_seconds,
                                lap3_time_seconds = EXCLUDED.lap3_time_seconds,
                                lap4_time_seconds = EXCLUDED.lap4_time_seconds
                        """)
                        execute_values(cursor, insert_heat_participants_query, heat_participants_data)

            # Commit the transaction for this file
            conn.commit()
            logging.info(f"Successfully processed and committed data for file: {filename}")

            # Move the processed file to the processed directory
            new_filepath = os.path.join(processed_dir, filename)
            try:
                shutil.move(filepath, new_filepath)
                logging.info(f"Moved processed file to: {new_filepath}")
            except shutil.Error as e:
                logging.error(f"Error moving processed file {filepath} to {new_filepath}: {e}")
            except OSError as e:
                logging.error(f"Error moving processed file {filepath} to {new_filepath}: {e}")


        except FileNotFoundError:
            logging.error(f"File not found: {filepath}")
            if conn:
                conn.rollback() # Rollback transaction on error
        except json.JSONDecodeError:
            logging.error(f"Error decoding JSON from file: {filepath}")
            if conn:
                conn.rollback() # Rollback transaction on error
        except Exception as e:
            logging.error(f"An unexpected error occurred while processing file {filename}: {e}")
            if conn:
                conn.rollback() # Rollback transaction on error
        finally:
             conn.autocommit = True # Reset autocommit

    cursor.close()


if __name__ == '__main__':
    db_conn = None
    try:
        db_conn = get_db_connection()
        transform_and_load(db_conn)
    except Exception as e:
        logging.critical(f"Data transformation process failed: {e}")
    finally:
        if db_conn:
            db_conn.close()
            logging.info("Database connection closed.")
