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

def database_connection(query):
    """takes a string SQL statement as input, and depending on the type of statement either performs an insert or returns data from the database"""

    try:
        connection = psycopg2.connect(user = DBUSER, password = DBPASS, host = DBIP, port = DBPORT, database = DB)
        cursor = connection.cursor()
        cursor.execute(query)

        if "SELECT" in query:
            returned_array = cursor.fetchall()
            cursor.close()
            connection.close()
            
            return returned_array

        else:
            connection.commit()
            cursor.close()
            connection.close()
            

    except (Exception, psycopg2.Error) as error:
        print('Error in database connection', error)
    

    finally:
        if(connection):
            cursor.close()
            connection.close()
            # print('database connection closed')

#check-functions for data correctness
def check_not_duplicate(subject_json, pub_check):

    """takes current subject's compiled json blob, checks if a dupe (if published record for that subject in that data_version exists) and returns boolean"""
    query = database_connection(f"SELECT * FROM ds_subjects_phenotypes WHERE subject_id = '{subject_json['subject_id']}' AND _data->>'data_version' = '{subject_json['data_version']}' AND {pub_check}")
    if query:
        return False
    else:
        return True

def check_not_dupe_baseline(subject_id, subject_type):
    """takes subject_id and subject_type, returns true if not a duplicate, false if record found with that subject_id/type in baseline table"""
    query = database_connection(f"SELECT * FROM ds_subjects_phenotypes_baseline WHERE subject_id = '{subject_id}' and subject_type = '{subject_type}'")
    if query:
        return False
    else:
        return True

def get_data_version_id(release_version):
    """takes string release_version and returns id from data_version table"""
    query = database_connection(f"SELECT id FROM data_versions WHERE release_version = '{release_version}'")
    try:
        return query[0][0]
    except:
        print(f"No id found for release_version {release_version}. Check that the data_version has been added to the database")
        # then need to do something like return a signal that there's a problem
        return 'release_version has not found in db'

def check_subject_exists(subject_type_view, subject_id, release_version):
    """takes id and release_version, makes sure that record exists for subject in target data version"""
    query = database_connection(f"SELECT COUNT(*) FROM {subject_type_view} where subject_id = '{subject_id}' AND release_version = '{release_version}'")

    if query[0][0] > 0:
        return True
    else:
        return False
        
# functions for handling user input
def get_filename():
    while True:
        try:
            filename_input = input(f"Enter loadfile name. ")
        except ValueError:
            continue
        if len(filename_input) < 4:
            print('Please enter a valid filename.')
            continue
        else:
            if '.csv' not in filename_input:
                print("Please make sure you've uploaded a .csv file.")
                continue
            else:
                filename = filename_input
                break

    return filename
        
def get_subject_type():
    """takes nothing, returns subject_type for data being handled"""
    while True:
        try:
            casefam_input = input(f"Are you uploading family data? ")
        except ValueError:
            continue
        if casefam_input in ['y', 'Y', 'yes', 'Yes', 'YES']:
            user_input_subject_type = 'family'
            print("Loading family data.")
            return user_input_subject_type
        elif casefam_input in ['n', 'N', 'no', 'No', 'NO']:
            user_input_subject_type = 'case/control'                
            print("Loading case/control data.")
            return user_input_subject_type
        else:
            print("Please input a valid entry. ")
            continue

def get_publish_action():
    """takes nothing returns boolean value for publish status based on user input"""
    while True:
        try:
            pubstat_input = input(f"Do you want loaded data to be published? ")
        except ValueError:
            continue
        if pubstat_input in ['y', 'Y', 'yes', 'Yes', 'YES']:
            publish_status = True
            print("Loaded records will be given publish status.")
            return publish_status

        elif pubstat_input in ['n', 'N', 'no', 'No', 'NO']:
            print("Loaded records will no be published.")
            publish_status = False
            return publish_status
        else:
            print("Please input a valid entry. ")
            continue

# log generators
def generate_errorlog():
    """creates error log and writes to 'log_files' directory"""
    if len(error_log) > 0:
        timestamp = calendar.timegm(time.gmtime())
        f = open(f'./log_files/{timestamp}-log.txt', 'w+')
        f.write(f'{str(len(error_log.items()))} flag(s) raised in runtime. See details below: \n\n')
        for key, value in error_log.items():
            f.write(f'Error: {value[1]} \n')

def generate_success_list():
    """creates a list of successfully created and inserted ADSP IDs"""

    if len(success_id_log) > 0:
        timestamp = calendar.timegm(time.gmtime())
        f = open(f'./log_files/success_lists/{timestamp}-generated_ids.txt', 'w+')
        for id in success_id_log:
            if success_id_log.index(id) >= len(success_id_log)-1:
                f.write(id)
            else:
                f.write(id + ', ')

        f.close()

