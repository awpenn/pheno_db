import sys
sys.path.append('/id_db/.venv/lib/python3.6/site-packages')

import psycopg2
import csv
from dotenv import load_dotenv
import os
import json

import pandas as pd

from pheno_utils import *


def main():
    # user_input_subject_type = get_subject_type()
    ##hardcoded values for dev
    user_input_subject_type = 'case/control'
    # LOADFILE = get_filename()
    ##hardcoded values for dev
    LOADFILE = 'starting_data_for_test.csv'

    data_dict = create_data_dict( LOADFILE, user_input_subject_type )
    dict_name = database_connection(f"SELECT dictionary_name FROM env_var_by_subject_type WHERE subject_type = '{ user_input_subject_type }'")[ 0 ][ 0 ]
    dictionary = get_dict_data( dict_name )

    run_checks( data_dict, dictionary )


def create_data_dict( LOADFILE, subject_type ):
    """takes loadfile, subject_type as args, returns dict of json data keyed by subject id of data to be valcheck"""
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
                        blob["data_version"] = get_data_version_id( value )
                    if headers[index].lower() == 'latest_update_version':
                        blob["latest_update_version"] = get_data_version_id( value )
                    
                if type(blob["data_version"]) == int:
                    data_dict[f'{blob["subject_id"]}_{blob["release_version"]}'] = blob
                else:
                    print(f"Version {blob['data_version']} not found. Record will not be added. Check database.")


    for key, record in data_dict.items():
        """remove subject id from blob for each record in dict"""
        record.pop('subject_id')
        record.pop('release_version')

    return data_dict

def run_checks( data_dict, dictionary ):
    """takes data_dict and dictionary as args, checks that all values in data are valid, returns ???"""

    for key, value in data_dict.items():
        for phenotype, pheno_value in value.items():
            ## gets values_dict in dictionary by the current phenotype in current subject entry in data_dict.
            try:
                dict_values = dictionary.data_values[ dictionary.variable == f'{ phenotype }' ].values[ 0 ]
                check_data_value( key, dict_values, phenotype, pheno_value )
            except:
                print(f"{ phenotype } does not appear in the dictionary.")

## this is almost exactly same as the function in load_phenotypes, so consider generalizing original and calling across files          
def check_data_value( subject_id, dict_values, phenotype, pheno_value ):
    """takes dict_values, phenotype, pheno_value, returns True if everythings cool, False if not"""
    if 'age' in phenotype:
        try:
            if int( pheno_value ) not in range( 121 ):
                print(f"{pheno_value} in not valid")
                return False
        except:
            if pheno_value != 'NA':
                print(f"{pheno_value} invalid")
                return False
    else:
        if len( dict_values ) > 1:

            if str(pheno_value) in dict_values:
                print(f'{ phenotype } value { pheno_value } is valid. { dict_values }')
            else:
                print(f'{ phenotype } value { pheno_value } is NOT valid. { dict_values }')
        else:
            print(f'{ phenotype } has no dict values and doesnt need to be checked')


if __name__ == '__main__':
    main() 