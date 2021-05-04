import sys
sys.path.append('/home/pheno_db/.venv/lib/python3.6/site-packages/')

import psycopg2
import csv
from dotenv import load_dotenv
import os
import json
import datetime

import pandas as pd

import pheno_utils
from Subjects import *
    
def main():
    ## dict of class names for different subject_types to pass into update_checks function
    classname_dict = { subjname: classname for ( subjname, classname ) in pheno_utils.database_connection( "SELECT subject_type, subject_classname FROM env_var_by_subject_type", ( ) ) }

    user_input_subject_type = pheno_utils.get_subject_type()
    LOADFILE = pheno_utils.get_filename()

    ## data from comparison csv now in subjid-keyed json
    comparison_data = create_data_dict( LOADFILE, user_input_subject_type )
    checked_data = run_update_checks( comparison_data, classname_dict, user_input_subject_type )
    
    for key, value in checked_data.items():
        if 'data_errors' in value.keys():
            ## have to flip the dataframe columns/rows with .transpose()
            df = pd.read_json( json.dumps( checked_data ) ).transpose()
            df = df.reindex( columns = list( value.keys( ) ) )

            pheno_utils.create_tsv( df, user_input_subject_type, validation_type = 'update_validation' )
            print(f"One or more data errors found in { LOADFILE }. A tsv with error flags will be generated.")
            ## Found an error, generated the tsv and now will exit. 
            sys.exit()

    print(f"No data errors found in { LOADFILE }.")

def create_data_dict( LOADFILE, subject_type ):
    """takes loadfile, subject_type as args, returns dict of json data keyed by subject id of data to be valcheck"""
    release_dict = pheno_utils.build_release_dict()
    data_dict = {}

    with open(f'./source_files/{LOADFILE}', mode='r', encoding='utf-8-sig') as csv_file:
        """"get the relationship table names and indexes from the csv file headers"""
        pheno_file = csv.reader(csv_file)
        headers = next(pheno_file)
        
        for row in pheno_file:
            if pheno_file.line_num > 1:
                blob = {}
                for index, value in enumerate( row ):
                    try:
                        blob[ headers[ index ].lower( ) ] = int( value )
                    except:
                        blob[ headers[ index ].lower( ) ] = value.strip( )

                ## generated comparison file and database want 'subject_id', 
                ## legacy data has 'subjid' so this just prevents key error if it's the wrong one in the file being processed
                try:
                    data_dict[ blob[ "subject_id" ] ] = blob
                except KeyError:
                    data_dict[ blob[ "subjid" ] ] = blob

    return data_dict

def run_update_checks( comparison_data, classname_dict, subject_type ):
    """takes the jsonified comparison_data, the appropriate dictionary, subject_type, and classname_dict..."""
    review_dict = {}
    for key, value in comparison_data.items():
        reviewed_subject_object = value
        
        ## uses the etattr to get the reference to class based on classname from dict, then instantiates subject object with 'value' data, last is checktype to indicate what checks and what data present in subject data object
        subject = getattr( sys.modules[ __name__ ], classname_dict[ subject_type ] )( key, value, comparison_data, "update-validation" )

        subject_data_errors = subject.run_update_validation_checks()

        if subject_data_errors:
            ## if there are errors, create a 'data_errors' variable, convert errors into a string, and store with the rest of the comparison data
            reviewed_subject_object[ 'data_errors' ] = '; '.join( [ f"{ x[ 0 ] }: { x[ 1 ] }" for x in subject_data_errors.items() ] )
        # else:
        #     reviewed_subject_object[ 'data_errors' ] = ''

        review_dict[ key ] = reviewed_subject_object

    return review_dict

if __name__ == '__main__':
    print('start ', datetime.datetime.now().strftime("%H:%M:%S") )
    main()  
    print('end ', datetime.datetime.now().strftime("%H:%M:%S") )