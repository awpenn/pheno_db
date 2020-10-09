import sys
sys.path.append('/id_db/.venv/lib/python3.6/site-packages')

import psycopg2
import csv
from dotenv import load_dotenv
import os
import json

import calendar
import time

new_records = []
success_id_log = []
error_log = {}

load_dotenv()
DBIP = os.getenv('DBIP')
DBPASS = os.getenv('DBPASS')
DBPORT = os.getenv('DBPORT')
DB = os.getenv('DB')
DBUSER = os.getenv('DBUSER')
LOADFILE = ''

family_data_creation = False
publish_status = False

def main():
    """main conductor function for the script.  Takes some input about the type of data being uploaded and runs the process from there."""
    global LOADFILE
   
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
        global family_data_creation
        while True:
            try:
                casefam_input = input(f"Are you uploading family data? ")
            except ValueError:
                continue
            if casefam_input in ['y', 'Y', 'yes', 'Yes', 'YES']:
                family_data_creation = True
                print("Loading family data.")
                break
            elif casefam_input in ['n', 'N', 'no', 'No', 'NO']:
                print("Loading case/control data.")
                break
            else:
                print("Please input a valid entry. ")
                continue
    

    def get_publish_status():
        global publish_status
        while True:
            try:
                pubstat_input = input(f"Do you want loaded data to be published? ")
            except ValueError:
                continue
            if pubstat_input in ['y', 'Y', 'yes', 'Yes', 'YES']:
                publish_status = True
                print("Loaded records will be given publish status.")
                break
            elif pubstat_input in ['n', 'N', 'no', 'No', 'NO']:
                print("Loaded records will no be published.")
                break
            else:
                print("Please input a valid entry. ")
                continue
    
    get_subject_type()
    get_publish_status()
    LOADFILE = get_filename()
    data_dict = create_data_dict(LOADFILE)
    write_to_db(data_dict)

def write_to_db(data_dict):
    """takes data dict and writes to database"""
    global family_data_creation
    global publish_status

    for key, value in data_dict.items():
        """key is id + version, so cuts off version part to get id"""
        split = key.index("_")
        subject_id = key[:split]
        _data = json.dumps(value)

        if family_data_creation:
            database_connection(f"INSERT INTO ds_subjects_phenotypes(subject_id, _data, subject_type, published) VALUES('{subject_id}', '{_data}', 'family', {publish_status})")
            save_baseline(subject_id, value)
        else:
            database_connection(f"INSERT INTO ds_subjects_phenotypes(subject_id, _data, subject_type, published) VALUES('{subject_id}', '{_data}', 'case/control', {publish_status})")
            save_baseline(subject_id, value)

def save_baseline(subject_id, data):
    """takes data dict and writes to database"""
    global family_data_creation

    _baseline_data = create_baseline_json(data)

    if family_data_creation:
        if check_not_dupe_baseline(subject_id, 'family'):
            database_connection(f"INSERT INTO ds_subjects_phenotypes_baseline(subject_id, _baseline_data, subject_type) VALUES('{subject_id}', '{_baseline_data}', 'family')")
        else:
            print(f'There is already a family baseline record for {subject_id}.')
    else:
        if check_not_dupe_baseline(subject_id, 'case/control'):
            database_connection(f"INSERT INTO ds_subjects_phenotypes_baseline(subject_id, _baseline_data, subject_type) VALUES('{subject_id}', '{_baseline_data}', 'case/control')") 
        else:
            print(f'There is already a case/control baseline record for {subject_id}.')

def create_data_dict(LOADFILE):
    """takes loadfile name as arg, returns dict of json data keyed by subject id of data to be entered in database"""
    data_dict = {}
    with open(f'./source_files/{LOADFILE}', mode='r', encoding='utf-8-sig') as csv_file:
        """"get the relationship table names and indexes from the csv file headers"""
        pheno_file = csv.reader(csv_file)
        headers = next(pheno_file)
        
        for row in pheno_file:
            if pheno_file.line_num > 1:
                blob = {}
                for index, value in enumerate(row):
                    try:
                        blob[headers[index].lower()] = int(value)
                    except:
                        blob[headers[index].lower()] = value
                    if headers[index].lower() == 'release_version':
                        blob["data_version"] = get_data_version_id(value)
                    if headers[index].lower() == 'latest_update_release_version':
                        blob["latest_update_version"] = get_data_version_id(value)

                if type(blob["data_version"]) == int:
                    data_dict[f'{blob["subject_id"]}_{blob["release_version"]}'] = blob
                else:
                    print(f"Version {blob['data_version']} not found. Record will not be added. Check database.")


    for key, record in data_dict.items():
        """remove subject id from blob for each record in dict"""
        record.pop('subject_id')
        record.pop('release_version')
        record.pop('latest_update_release_version')

    return data_dict

def create_baseline_json(data):
    """takes dict entry for subject being added to database and creates the copy of data for baseline table, returning json string"""
    baseline_data = {}
    for key, value in data.items():
        if "update" not in key and "correction" not in key:
            baseline_data[f"baseline_{key}"] = value
 
    return json.dumps(baseline_data)

def database_connection(query):
    """takes a string SQL statement as input, and depending on the type of statement either performs an insert or returns data from the database"""

    try:
        connection = psycopg2.connect(user = DBUSER, password = DBPASS, host = DBIP, port = DBPORT, database = DB)
        cursor = connection.cursor()
        cursor.execute(query)

        if "INSERT" in query:
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

def get_data_version_id(release_version):
    """takes string release_version and returns id from data_version table"""
    query = database_connection(f"SELECT id FROM data_versions WHERE release_version = '{release_version}'")
    try:
        return query[0][0]
    except:
        print(f"No id found for release_version {release_version}. Check that the data_version has been added to the database")
        # then need to do something like return a signal that there's a problem
        return release_version

def check_not_dupe_baseline(subject_id, subject_type):
    """takes subject_id and subject_type, returns true if not a duplicate, false if record found with that subject_id/type in baseline table"""
    query = database_connection(f"SELECT * FROM ds_subjects_phenotypes_baseline WHERE subject_id = '{subject_id}' and subject_type = '{subject_type}'")
    if query:
        return False
    else:
        return True

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

if __name__ == '__main__':
    main()
    # generate_errorlog()
    # generate_success_list()