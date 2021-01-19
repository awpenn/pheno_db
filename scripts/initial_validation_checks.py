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
from Subjects import *

def main():
    ## 1/15/21 testing
    # user_input_subject_type = 'case/control'
    # LOADFILE = 'cc-published.csv'
    user_input_subject_type = get_subject_type()
    LOADFILE = get_filename()
    
    ## dict gives the Class Object Names for each subject_type corresponding to user-input subject type selection
    classname_dict = { subjname: classname for ( subjname, classname ) in database_connection("SELECT subject_type, subject_classname FROM env_var_by_subject_type") }

    variables_match_dictionary, msg = check_loadfile_correctness( LOADFILE, user_input_subject_type )
    
    if not variables_match_dictionary:
        print( msg )
        sys.exit()
        
    else:
        print( msg )   
        
        data_dict = create_comparison_data_dict( LOADFILE, user_input_subject_type )

        reviewed_dict = run_checks( data_dict, classname_dict, user_input_subject_type )
        for key, value in reviewed_dict.items():

            if 'data_errors' in value.keys():
                ## have to flip the dataframe columns/rows with .transpose()
                df = pd.read_json( json.dumps( reviewed_dict ) ).transpose()
                create_tsv( df, user_input_subject_type )
                print(f"One or more data errors found in { LOADFILE }. A tsv with error flags will be generated.")
                ## Found an error, generated the tsv and now will exit. 
                sys.exit()

        print(f"No data errors found in { LOADFILE }.")

def run_checks( data_dict, classname_dict, subject_type ):
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
                    ## make sure its not a case of a #+ (eg 90+) value given
                    try:
                        if int( pheno_value.replace("+", "") ) not in range( 121 ):
                            return [ pheno_value, f"'{pheno_value}' is NOT valid for { phenotype }" ]
                        else:
                            return pheno_value

                    except:
                        return [ pheno_value, f"'{pheno_value}' is NOT valid for { phenotype }" ]
        
        else:
            if len( dict_values ) > 0:

                if str(pheno_value) in dict_values:
                    return pheno_value
                else:
                    return [ pheno_value, f"'{ pheno_value }' is NOT valid for { phenotype }." ]
            else:
                return pheno_value

    ## this will hold the dict info plus any errors that are found
    review_dict = {}

    for key, value in data_dict.items():
        reviewed_subject_object = value
        subject = getattr( sys.modules[ __name__ ], classname_dict[ subject_type ] )( value, data_dict, "initial-validation" )
        
        subject_data_errors = subject.run_initial_validation_checks()
        
        if subject_data_errors:
            ## if there are errors, create a 'data_errors' variable, convert errors into a string, and store with the rest of the comparison data
            reviewed_subject_object[ 'data_errors' ] = '; '.join( [ f"{ x[ 0 ] }: { x[ 1 ] }" for x in subject_data_errors.items() ] )

        review_dict[ key ] = reviewed_subject_object

    return review_dict

if __name__ == '__main__':
    print('start ', datetime.datetime.now().strftime("%H:%M:%S") )
    main()  
    print('end ', datetime.datetime.now().strftime("%H:%M:%S") )