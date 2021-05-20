import sys
sys.path.append('/home/pheno_db/.venv/lib/python3.6/site-packages/')

import psycopg2
import csv
from dotenv import load_dotenv
import os
import json
import datetime

import calendar
import time

import pheno_utils

user_input_subject_type = ''
script_name = 'legacy_loader.py'

def main():
    """main conductor function for the script.  Takes some input about the type of data being uploaded and runs the process from there."""
    global user_input_subject_type
    
    user_input_subject_type = pheno_utils.get_subject_type( )

    data_version = pheno_utils.user_input_data_version( )

    LOADFILE = pheno_utils.get_filename( )

    variables_match_dictionary, msg = check_legacy_loadfile_correctness( LOADFILE, user_input_subject_type )
    
    if not variables_match_dictionary:
        print( msg )
        sys.exit( )
        
    else:
        print( msg )
        ## 12/15 create_data_dict generalized and moved to utils
        data_dict = pheno_utils.create_data_dict( LOADFILE, user_input_subject_type, data_version, script_name )
        legacy_write_to_db( data_dict, data_version )
    
def legacy_write_to_db( data_dict, data_version_string ):
    """takes data dict and writes to database"""

    global user_input_subject_type
    data_version = None ## saving data version id from dict so can set publish status in data_version table after upload
    write_counter = 0

    for key, value in data_dict.items():
        data_version = value[ "data_version" ] ## saving data version id from dict so can set publish status in data_version table after upload

        try: ## have to deal with subjid vs subject_id
            subject_id = value.pop( 'subjid' )
        except KeyError:
            subject_id = value.pop( 'subject_id' )

        _data = json.dumps( value )
        try:
            pheno_utils.database_connection( f"INSERT INTO ds_subjects_phenotypes(subject_id, _data, subject_type, published) VALUES(%s, %s, '{ user_input_subject_type }', TRUE)", ( subject_id, _data ) )
            save_legacy_baseline( subject_id, value )
            write_counter += 1

        except:
            print(f"Error making entry for { subject_id } in { data_version_string }")

    ## ask user if ready to publish dataset, if yes, will flip publish boolean in data versions table
    if write_counter > 0:
        if user_input_publish_dataset( data_version_string, write_counter ):
            pheno_utils.change_data_version_published_status( "TRUE", data_version )
        else:
            print( f"Phenotype records published for { data_version_string } and cannot be changed, but version not published." )
            pheno_utils.change_data_version_published_status( "FALSE", data_version )
    else:
        print( "No records entered in database." )

def create_legacy_baseline_json( data ):
    """takes dict entry for subject being added to database and creates the copy of data for baseline table, returning json string"""
    baseline_data = {}
    for key, value in data.items( ):
        if "update" not in key and "correction" not in key:
            baseline_data[ key ] = value
 
    return json.dumps( baseline_data )

def save_legacy_baseline( subject_id, data ):
    """takes data dict and writes to database"""
    global user_input_subject_type

    _baseline_data = create_legacy_baseline_json( data )

    pheno_utils.database_connection( f"INSERT INTO ds_subjects_phenotypes_baseline(subject_id, _baseline_data, subject_type) VALUES(%s, %s, '{ user_input_subject_type }')", ( subject_id, _baseline_data ) ) 

def check_legacy_loadfile_variables_match_dictionary( data_dict, dictionary, subject_type, LOADFILE ):
    """Gets the appropriate dictionary, checks phenotype variables in are correct and complete"""
    keys_to_remove = ['update', 'correction', 'data_version', 'release_version', 'base_', 'subjid']
    dictionary_vars = [ var for var in dictionary[ "variable" ] ]
    modified_dictionary_varlist = []
    modified_dictionary_varlist.extend( ['correction', 'update_latest', 'update_adstatus', 'update_baseline'] )

    ## use next/iter to get first key in data_dict(ie. a subject id), then gets the keys for that subjects phenotype data dict
    data_dict_vars = [ var for var in data_dict[ next( iter( data_dict ) ) ].keys( ) ]

    for var in dictionary_vars:
        if var  != 'duplicate_subjid':
            if not any( keys in var for keys in keys_to_remove ):                
                modified_dictionary_varlist.append( var )
        else:
            modified_dictionary_varlist.append( var )

    if len( data_dict_vars ) == len( modified_dictionary_varlist ):
        for var in data_dict_vars:
            if var in modified_dictionary_varlist:
                continue
            else:
                return 0, f'"{ var }" is not in the { subject_type } dictionary.  Please check the correctness of your loadfile.'
        
        return 1, f'Variables in { LOADFILE } match those in dictionary for { subject_type }'

    else:
        ## if loadfile has more vars than dict
        if len( data_dict_vars ) > len( modified_dictionary_varlist ):
            var_diff = set( data_dict_vars ).difference( set( modified_dictionary_varlist ) )  
            return 0, f'{ LOADFILE } contains the following variable(s) not found in the dictionary: { var_diff }.  Please check loadfile for correctness.'
        else:
            var_diff = set( modified_dictionary_varlist ).difference( set( data_dict_vars ) )
            return 0, f'the following dictionary variables are missing from { LOADFILE }: { var_diff }.  Please check loadfile for correctness.'

def check_legacy_loadfile_correctness( LOADFILE, user_input_subject_type ):
    """takes loadfile and subject type, returns boolean indicating loadfile matches appropriate dict, along with a message"""
    """moved from initial_validation_check so can be used at beginning of any script"""
    data_dict = pheno_utils.create_comparison_data_dict( LOADFILE, user_input_subject_type )
    dict_name = pheno_utils.database_connection( f"SELECT dictionary_name FROM env_var_by_subject_type WHERE subject_type = '{ user_input_subject_type }'", ( ) )[ 0 ][ 0 ]
    dictionary = pheno_utils.get_dict_data( dict_name )

    variables_match_dictionary, msg = check_legacy_loadfile_variables_match_dictionary( data_dict, dictionary, user_input_subject_type, LOADFILE )
    
    return variables_match_dictionary, msg

if __name__ == '__main__':
    main()
