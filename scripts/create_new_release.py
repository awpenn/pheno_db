"""
Beginning point in updated workflow to begin a new release.
- create new release in versions table
- copy over previous latest published release subjects into new release
"""

import sys
sys.path.append('/home/pheno_db/.venv/lib/python3.6/site-packages/')

import csv
from dotenv import load_dotenv
import os
import json
import datetime

import flagchecks
import pheno_utils

script_name = 'create_new_release.py'

def main( ):

    release_name, release_tablekey = user_input_new_release_data( )

    user_input_subject_type = pheno_utils.get_subject_type( )

    subjects_from_previous_release = get_previous_release_data( subject_type = user_input_subject_type, new_data_version_tablekey = release_tablekey )

    print( f"Beginning data load: { datetime.datetime.now( ).strftime('%H:%M:%S') }" )
    
    write_to_db( data_dict = subjects_from_previous_release, user_input_subject_type = user_input_subject_type )

    print( f"Data load finished: { datetime.datetime.now( ).strftime('%H:%M:%S') }" )

def user_input_new_release_data( ):
    """no args, requests new release name and saves to database, returns string of user-entered release name and string of version id created by database"""
    existing_releases = [ release[ 0 ] for release in pheno_utils.database_connection( "SELECT release_version FROM data_versions", ( ) ) ]

    def save_new_release( release_name, release_date ):
        """takes string name and string data for new release, saves to database, returns generated table id"""
        try:
            pheno_utils.database_connection( f"INSERT INTO data_versions( release_version, version_date ) VALUES(%s, %s)", ( release_name, release_date ) )
        except:
            print( f'Error saving { release_name } to database.' )
            sys.exit( )
        return str( pheno_utils.database_connection( f"SELECT id FROM data_versions WHERE release_version = %s", ( release_name, ) )[ 0 ][ 0 ] )

    ## get release name
    def get_release_name( ):
        while True:
            try:
                release_name_input = input( f"Enter release name for new data version " )
            except ValueError:
                continue
            if not release_name_input:
                continue
            elif release_name_input.strip( ) in existing_releases:
                print( f'{ release_name_input } is already in the database. Please enter a new unique name.' )
                continue
            else:
                while True:
                    try:
                        confirm_input = input( f"Confirm { release_name_input } is correct name for new release?(y/n) " )
                    except ValueError:
                        continue
                    if not confirm_input:
                        continue
                    elif confirm_input.lower( ) not in [ 'y', 'n' ]:
                        continue
                    else:
                        if confirm_input.lower( ) == 'y':
                            return release_name_input
                        else:
                            break    
                continue

    ## get release date
    def get_release_date( ):
        while True:
            try:
                release_date_input = input( f"Enter release date (YYYY) for new data version " )
            except ValueError:
                continue
            if not release_date_input:
                continue
            elif not release_date_input.isdigit( ):
                print( f'Please enter a four-digit numerical value representing the year of the release.' )
                continue
            elif len( release_date_input ) != 4:
                print( f'Please enter a four-digit numerical value representing the year of the release.' )
                continue
            else:
                while True:
                    try:
                        confirm_input = input( f"Confirm { release_date_input } is correct date for new release?(y/n) " )
                    except ValueError:
                        continue
                    if not confirm_input:
                        continue
                    elif confirm_input.lower( ) not in [ 'y', 'n' ]:
                        continue
                    else:
                        if confirm_input.lower( ) == 'y':
                            return release_date_input
                        else:
                            break    
                continue
        
    release_name = get_release_name( )
    release_date = get_release_date( )

    release_tablekey = save_new_release( release_name, release_date )

    return release_name, release_tablekey

def get_previous_release_data( subject_type, new_data_version_tablekey ):
    """takes subject type string and new data_version id string as args, returns...."""
    ## get correct latest_published view
    latest_published_view = pheno_utils.database_connection( f"SELECT latest_published_view_name FROM env_var_by_subject_type WHERE subject_type = '{ subject_type }'", ( ) )[ 0 ][ 0 ]
    
    ## get the data_version tablekey value for the latest published version
    latest_published_data_version = str( pheno_utils.database_connection( f"SELECT data_version FROM { latest_published_view }", ( ) )[ 0 ][ 0 ] )
    
    ## get all the records from ds_subjects_phenotypes with that data_version value
    query = f"SELECT subject_id, _data FROM ds_subjects_phenotypes WHERE _data->>'data_version' = '{ latest_published_data_version }' AND subject_type = '{ subject_type }'"

    phenotype_data = { record[ 0 ]: record[ 1 ] for record in pheno_utils.database_connection( query , ( ) ) }

    ## update the data_version value in returned dict
    for id, data in list( phenotype_data.items( ) ):
        phenotype_data[ id ][ 'data_version' ] = new_data_version_tablekey

    return phenotype_data

def write_to_db( data_dict, user_input_subject_type ):
    """takes data dict and subject_type (str), and writes to database"""

    requires_ad_status_check = ['case/control', 'family']
    requires_diagnosis_update_check = ['ADNI', 'PSP/CDB']

    ## gets list of subjects in baseline table of type matching the user_input_subject_type, so can see if have to add to baseline table
    baseline_dupecheck_list = pheno_utils.build_baseline_dupcheck_list( user_input_subject_type )

    ## dicts for dbcall-less flag updates
    update_baseline_dict = flagchecks.build_update_baseline_check_dict( user_input_subject_type )
    update_latest_dict = flagchecks.build_update_latest_dict( user_input_subject_type )

    if user_input_subject_type in requires_ad_status_check:
        adstatus_check_dict = flagchecks.build_adstatus_check_dict( user_input_subject_type )
    
    if user_input_subject_type in requires_diagnosis_update_check:
        diagnosis_update_check_dict = flagchecks.build_update_diagnosis_check_dict( user_input_subject_type )
    
    for key, value in data_dict.items():
        subject_id = key
        value[ "update_baseline" ] = flagchecks.update_baseline_check( subject_id , value, update_baseline_dict )
        value[ "update_latest" ] = flagchecks.update_latest_check( subject_id, value, update_latest_dict )
        value[ "correction" ] = flagchecks.correction_check( value )

        if user_input_subject_type in requires_ad_status_check:
            value[ "update_adstatus" ] = flagchecks.update_adstatus_check( subject_id, value[ "ad" ], adstatus_check_dict )     

        if user_input_subject_type in requires_diagnosis_update_check:
            value[ "update_diagnosis" ] = flagchecks.update_diagnosis_check( subject_id, user_input_subject_type, value, diagnosis_update_check_dict )

        _data = json.dumps( value )

        if not pheno_utils.DEBUG:
            try:
                pheno_utils.database_connection( f"INSERT INTO ds_subjects_phenotypes(subject_id, _data, subject_type) VALUES(%s, %s, '{ user_input_subject_type }')", ( subject_id, _data ) )
            except:
                err = f'ERROR: Error adding update for { subject_id } to database.'
                print( err )
                pheno_utils.error_log[ len( pheno_utils.error_log ) + 1 ] = [ err ]
        else:
            print( 'DEBUG mode, no write to db....\n')
        ## add subject_id back in for reporting
        data_dict[ key ][ 'subject_id' ] = subject_id

    pheno_utils.generate_summary_report( data_dict = data_dict, user_input_subject_type = user_input_subject_type, loadtype = 'unpublished_update' )

if __name__ == '__main__':
    pass
    main( )
