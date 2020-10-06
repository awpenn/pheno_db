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
compare_family_data = False

def main():
    """main conductor function for the script."""
    def get_subject_type():
        global compare_family_data
        while True:
            try:
                casefam_input = input(f"Are comparing family data? ")
            except ValueError:
                continue
            if casefam_input in ['y', 'Y', 'yes', 'Yes', 'YES']:
                compare_family_data = True
                print("Loading family data.")
                break
            elif casefam_input in ['n', 'N', 'no', 'No', 'NO']:
                print("Loading case/control data.")
                break
            else:
                print("Please input a valid entry. ")
                continue
    
    get_subject_type()
    comparison_dict_and_headers = create_comparison_dict()

    build_comparison_table( comparison_dict_and_headers )

def create_comparison_dict():
    """takes no args and creates comparison dict and list of headers"""
    global compare_family_data
    compare_dict = {}
    headers_list = []

    if compare_family_data:
        data = database_connection("SELECT * FROM get_unpublished_updates_fam LEFT JOIN get_current_fam ON get_unpublished_updates_fam.subject_id = get_current_fam.subject_id")
        headers = database_connection("SELECT table_name, ordinal_position, column_name FROM information_schema.columns WHERE table_name in('get_unpublished_updates_cc' ,'get_current_fam');" )
    else:
        data = database_connection("SELECT * FROM get_unpublished_updates_cc LEFT JOIN get_current_cc ON get_unpublished_updates_cc.subject_id = get_current_cc.subject_id")
        headers = database_connection("SELECT ordinal_position, column_name, table_name FROM information_schema.columns WHERE table_name in('get_unpublished_updates_cc' ,'get_current_cc');")
    
    unique_headers_len = int( len( headers )/2 )

    for p_value in data:
        subject_id = p_value[ 0 ]
        subject_dict = {}

        for index, h_value in enumerate(headers):
            if index <= unique_headers_len-1:

                phenotype = h_value[ 1 ]
                headers_list.append( phenotype )
                update_val = p_value[ index ]
                current_val = p_value[ index + unique_headers_len]
                if update_val != current_val:
                    values = f"{update_val}, {current_val}"
                else:
                    values = ""
                subject_dict[phenotype] = values

        subject_dict.pop("subject_id")
        compare_dict[subject_id] = subject_dict

    return compare_dict, headers_list

def build_comparison_table(comparison_dict_and_headers):
    """takes comparison dict and list of headers, creates table for comparison"""
    comparison_dict = comparison_dict_and_headers[ 0 ]
    headers = comparison_dict_and_headers[ 1 ]

    with open('tada.csv', mode='w') as csv_file:
        fieldnames = headers
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        for subject_id, data in comparison_dict.items():
            data["subject_id"] = subject_id
            writer.writerow(data)

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



    