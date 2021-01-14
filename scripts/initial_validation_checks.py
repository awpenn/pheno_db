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
    user_input_subject_type = get_subject_type()
    LOADFILE = get_filename()

    variables_match_dictionary, msg = check_loadfile_correctness( LOADFILE, user_input_subject_type )
    
    if not variables_match_dictionary:
        print( msg )
        sys.exit()
        
    else:
        print( msg )   
        
        data_dict = create_comparison_data_dict( LOADFILE, user_input_subject_type )
        dict_name = database_connection(f"SELECT dictionary_name FROM env_var_by_subject_type WHERE subject_type = '{ user_input_subject_type }'")[ 0 ][ 0 ]
        dictionary = get_dict_data( dict_name )

        reviewed_dict = run_checks( data_dict, dictionary )
        for key, value in reviewed_dict.items():

            if 'data_errors' in value.keys():
                ## have to flip the dataframe columns/rows with .transpose()
                df = pd.read_json( json.dumps( reviewed_dict ) ).transpose()
                create_tsv( df, user_input_subject_type )
                print(f"One or more data errors found in { LOADFILE }. A tsv with error flags will be generated.")
                ## Found an error, generated the tsv and now will exit. 
                sys.exit()

        print(f"No data errors found in { LOADFILE }.")

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

        review_dict[ key ] = reviewed_subject_object

    return review_dict


if __name__ == '__main__':
    print('start ', datetime.datetime.now().strftime("%H:%M:%S") )
    main()  
    print('end ', datetime.datetime.now().strftime("%H:%M:%S") )