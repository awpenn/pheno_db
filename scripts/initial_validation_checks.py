import sys
sys.path.append('/home/pheno_db/.venv/lib/python3.6/site-packages/')

import psycopg2
import csv
from dotenv import load_dotenv
import os
import json
import datetime

import pandas as pd

from pheno_utils import *
from Subjects import *

def main():
    user_input_subject_type = get_subject_type()
    LOADFILE = get_filename()
    
    variables_match_dictionary, msg = check_loadfile_correctness( LOADFILE, user_input_subject_type )
    
    ## dict gives the Class Object Names for each subject_type corresponding to user-input subject type selection
    classname_dict = { subjname: classname for ( subjname, classname ) in database_connection("SELECT subject_type, subject_classname FROM env_var_by_subject_type") }

    
    if not variables_match_dictionary:
        print( msg )
        sys.exit()
        
    else:
        print( msg )   
        
        data_dict = create_comparison_data_dict( LOADFILE, user_input_subject_type )
        print('start checks ', datetime.datetime.now().strftime("%H:%M:%S") )  ##for testing, remove when done
        reviewed_dict = run_checks( data_dict, classname_dict, user_input_subject_type )
        for key, value in reviewed_dict.items():

            if 'data_errors' in value.keys():
                ## have to flip the dataframe columns/rows with .transpose()
                df = pd.read_json( json.dumps( reviewed_dict ) ).transpose()
                df.index.name = 'SUBJID'
                df = df.reindex( columns = list( value.keys( ) ) )
                create_tsv( df, user_input_subject_type, validation_type = 'initial_validation',  requires_index = True )
                print(f"One or more data errors found in { LOADFILE }. A tsv with error flags will be generated.")
                ## Found an error, generated the tsv and now will exit.\
                print('end checks ', datetime.datetime.now().strftime("%H:%M:%S") )
                sys.exit()

        print(f"No data errors found in { LOADFILE }.")

def run_checks( data_dict, classname_dict, subject_type ):
    """
    takes data_dict and dictionary as args, checks that all values in data are valid, 
    returns data as dict with data_errors attribute if errors found
    """
    ## this will hold the dict info plus any errors that are found
    review_dict = {}
    
    ##ask user if they want to turn any toggle-able checks off
    toggle_checks = user_input_toggle_checks( subject_type, checktype = "initial-validation" )

    for key, value in data_dict.items():
        reviewed_subject_object = value
        subject = getattr( sys.modules[ __name__ ], classname_dict[ subject_type ] )( key, value, data_dict, "initial-validation" )
        
        if toggle_checks:
            subject_data_errors = subject.run_initial_validation_checks( toggle_checks )
        else:
            subject_data_errors = subject.run_initial_validation_checks( )
        
        if subject_data_errors:
            ## if there are errors, create a 'data_errors' variable, convert errors into a string, and store with the rest of the comparison data
            reviewed_subject_object[ 'data_errors' ] = '; '.join( [ f"{ x[ 0 ] }: { x[ 1 ] }" for x in subject_data_errors.items() ] )

        review_dict[ key ] = reviewed_subject_object

    return review_dict

if __name__ == '__main__':
    # print('start ', datetime.datetime.now().strftime("%H:%M:%S") )
    main()  
    # print('end ', datetime.datetime.now().strftime("%H:%M:%S") )