import sys
sys.path.append('/id_db/.venv/lib/python3.6/site-packages')

import psycopg2
import csv
from dotenv import load_dotenv
import os
import json

error_log = {}

load_dotenv()
DBIP = os.getenv('DBIP')
DBPASS = os.getenv('DBPASS')
DBPORT = os.getenv('DBPORT')
DB = os.getenv('DB')
DBUSER = os.getenv('DBUSER')
LOADFILE = ''
compare_family_data = False

def update_baseline_check_legacy_data( subject_id, subject_type, data ):
    """take subject id, subject_type, and the data being loaded from the file, gets baseline, 
    removes keys that aren't in both, compares stringified JSON to see if changed,
    the 0 or 1 needed to fill in value
    """
    modified_update_dict = {}
    modified_baseline_dict = {}

    baseline_data = database_connection(f"SELECT _baseline_data FROM ds_subjects_phenotypes_baseline WHERE subject_id = '{subject_id}' AND subject_type = '{subject_type}'")

    try:
        for key, value in data.items():
            if 'update' not in key and 'correction' not in key and 'data_version' not in key:
                modified_update_dict[key] = value
    
        for key, value in baseline_data[0][0].items():
            if 'update' not in key and 'correction' not in key and 'data_version' not in key:
                modified_baseline_dict[key] = value
    except:
        print('This appears to be data for a subject not yet in the database.  No update from baseline to current will be indicated')
        return 0

    update_string = json.dumps(modified_update_dict)
    baseline_string = json.dumps(modified_baseline_dict)

    if update_string == baseline_string:
        return 0
    else: 
        return 1

def update_latest_check_legacy_data( subject_id, subject_type, data ):
    """takes subjectid, subject_type, and data dict, checks incoming against previous version, returns 0 or 1 for flag value in data object being written to db"""
    modified_update_dict = {}
    modified_previous_version_dict = {}
    
    previous_version_data = database_connection(f"SELECT _data FROM ds_subjects_phenotypes WHERE subject_id = '{subject_id}' AND subject_type = '{subject_type}' ORDER BY _data->>'data_version' DESC")
    
    for key, value in data.items():
        if 'update' not in key and 'correction' not in key and 'data_version' not in key:
            modified_update_dict[key] = value

    try:
        for key, value in previous_version_data[0][0].items():
            if 'update' not in key and 'correction' not in key and 'data_version' not in key:
                modified_previous_version_dict[key] = value

    except:
        print("There are no previous versions of this subject's phenotypes.  Update_latest flag set to 1")
        return 1

    for key, value in modified_update_dict.items():
        if value == modified_previous_version_dict[key]:
            continue
        else:
            print(f'different between new record and previous version found for {key}')
            return 1
    
    return 0


def update_adstatus_check_legacy_data( subject_id, subject_type, data ):
    """takes subject_id, subject_type, and data to be written to database, 
    checks ad value for baseline version, returns appropriate value for new data for adstatus flag"""

    baseline_data = database_connection(f"SELECT _baseline_data FROM ds_subjects_phenotypes_baseline WHERE subject_id = '{subject_id}' AND subject_type = '{subject_type}'")

    try:
        baseline_ad = baseline_data[0][0]["ad"]
    except:
        print(f'No {subject_type} baseline record found for {subject_id}.')
        return 0
    

    if data["ad"] == baseline_ad:
        return 0
    else:
        return 1



def database_connection(query):
    """takes a string SQL statement as input, and depending on the type of statement either performs an insert or returns data from the database"""

    try:
        connection = psycopg2.connect(user = DBUSER, password = DBPASS, host = DBIP, port = DBPORT, database = DB)
        cursor = connection.cursor()
        cursor.execute(query)

        if "INSERT" in query or "UPDATE" in query:
            connection.commit()
            cursor.close()
            connection.close()
            
        else:
            returned_array = cursor.fetchall()
            cursor.close()
            connection.close()
            
            return returned_array

    except (Exception, psycopg2.Error) as error:
        print('Error in database connection', error)
    

    finally:
        if(connection):
            cursor.close()
            connection.close()
            print('database connection closed')
    