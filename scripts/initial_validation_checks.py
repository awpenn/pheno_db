import sys
sys.path.append('/id_db/.venv/lib/python3.6/site-packages')

import psycopg2
import csv
from dotenv import load_dotenv
import os
import json
import datetime

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

    reviewed_dict = run_checks( data_dict, dictionary )
    for key, value in reviewed_dict.items():

        if 'data_errors' in value.keys():
            df = pd.read_json( json.dumps( reviewed_dict ) ).transpose()
            create_tsv( df, user_input_subject_type )
            print(f"One or more data errors found in { LOADFILE }. A tsv with error flags will be generated.")
            ## Found an error, generated the tsv and now will exit. 
            sys.exit()
        else:
            # print(f'Found no errors { key }')
            continue

    print(f"No data errors found in { LOADFILE }.")

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
    ##utils
    def check_data_value( subject_id, dict_values, phenotype, pheno_value ):
        """takes subject_id, dict_values, phenotype, pheno_value as args, 
        returns either the original value passed in as `pheno_value` or list with value and error message(?)
        """
        if 'age' in phenotype:
            try:
                if int( pheno_value ) not in range( 121 ):
                    return [ pheno_value, f"'{pheno_value}' is NOT valid for { phenotype }" ]
                else:
                    return pheno_value

            except:
                if pheno_value != 'NA':
                    return [ pheno_value, f"'{pheno_value}' is NOT valid for { phenotype }" ]
        
        else:
            if len( dict_values ) > 1:

                if str(pheno_value) in dict_values:
                    return pheno_value
                else:
                    return [ pheno_value, f"'{ pheno_value }' is NOT valid for { phenotype }." ]
            else:
                return pheno_value

    ## this will hold the dict info plus any errors that are found
    review_dict = {}

    for key, value in data_dict.items():
        reviewed_subject_object = {}
        error_list = []
        for phenotype, pheno_value in value.items():
            ## gets values_dict in dictionary by the current phenotype in current subject entry in data_dict.

            if pheno_value == '' and phenotype.lower() != 'comments':
                checked_phenotype_value = [ pheno_value, f"Blank value given for { phenotype }." ]

            else:
                try:
                    dict_values = dictionary.data_values[ dictionary.variable == f'{ phenotype }' ].values[ 0 ]

                except Exception as e:
                    # print(f"{ phenotype } does not appear in the dictionary { key }.")
                    reviewed_subject_object[ phenotype ] = pheno_value
                    continue

                checked_phenotype_value = check_data_value( key, dict_values, phenotype, pheno_value )

            if isinstance( checked_phenotype_value, list ):
                p_value, error_msg = checked_phenotype_value
                reviewed_subject_object [ phenotype ] = p_value
                error_list.append( error_msg )

            else:
                reviewed_subject_object[ phenotype ] = checked_phenotype_value

        ## if errors, take the compiled errors list, add to subject's data object, concatenating list items, separated by semi-col
        if len( error_list ) > 0:
            reviewed_subject_object[ 'data_errors' ] = '; '.join( error_list )

        review_dict[ key.split('_') [ 0 ] ] = reviewed_subject_object

    return review_dict

def create_tsv( dataframe, subject_type ):
    """takes the compiled dataframe and subject-type (to formulate filename) and creates a TSV."""
    datestamp = datetime.date.today()
    corrected_subject_type = subject_type.replace( "/", "+" )

    dataframe.to_csv(f"./comparison_files/{ corrected_subject_type }-validation_errors-{ datestamp }.txt",sep="\t",index=True)
if __name__ == '__main__':
    main()  