import sys
sys.path.append('/home/pheno_db/.venv/lib/python3.6/site-packages/')

from load_phenotypes import database_connection

import psycopg2
import csv
from dotenv import load_dotenv
import os
import json

error_log = {}

#flag-checkers for legacy data loading

## update_baseline
def build_update_baseline_check_dict( subject_type ):
    """takes subject_type, returns dict keyed by subject with baseline data matching correct subject_type"""
    _baseline_data = database_connection(f"SELECT subject_id, _baseline_data FROM ds_subjects_phenotypes_baseline WHERE subject_type = '{ subject_type }'")

    ## take data and transform into dict keyed by subject_id
    update_baseline_dict = { record[ 0 ]:  record[ 1 ] for record in _baseline_data }
    
    return update_baseline_dict

def update_baseline_check( subject_id, data, update_baseline_dict ):
    """take subject id, the data being loaded from the file, the compiled baseline_dict, 
    removes keys that aren't in both, compares stringified JSON to see if changed,
    the 0 or 1 needed to fill in value
    """
    
    keys_to_remove = ['update', 'correction', 'data_version', 'release_version']
    modified_update_dict = {}
    modified_baseline_dict = {}

    for key, value in data.items():
        ##if current key in iter over data dict is NOT in the keys_to_remove list, add to mod dict
        if not any( keys in key for keys in keys_to_remove ):
            modified_update_dict[ key ] = value

    try:
        for key, value in update_baseline_dict[ subject_id ].items():
            if not any( keys in key for keys in keys_to_remove ):
                modified_baseline_dict[ key ] = value
                
    except:
        print('This appears to be data for a subject not yet published in the database.  No update from baseline to current will be indicated')
        return 0

    for key, value in modified_update_dict.items():
        if str( value ) == str( modified_baseline_dict[key] ):
            continue
        else:
            print(f'{ subject_id }: difference between new record and baseline version found for { key }')
            return 1
    
    return 0

## update_latest
def build_update_latest_dict( subject_type ):
    """takes subject_type, gets all subject data from [type]_current view for particular subject type, keyed by subject_id"""

    current_view = database_connection(f"SELECT current_view_name FROM env_var_by_subject_type WHERE subject_type = '{ subject_type }' ")[ 0 ][ 0 ]    
    try:
        most_recent_published_data_version = database_connection(f"SELECT DISTINCT data_version FROM { current_view } ORDER BY data_version DESC")[ 0 ][ 0 ]
    except:
        print(f'No published data from { subject_type } subjects.')
        return {}
    _latest_data = database_connection(f"SELECT subject_id, _data FROM ds_subjects_phenotypes \
        WHERE subject_type = '{ subject_type }' AND published = 'TRUE' AND _data->>'data_version' = '{ most_recent_published_data_version }'")

    update_latest_dict = { record[ 0 ]:  record[ 1 ] for record in _latest_data }

    return update_latest_dict

def update_latest_check( subject_id, data, update_latest_dict ):
    """takes subject_id, data being loaded, and compiled update_latest_dict, 
    checks incoming against data in update_latest_dict, returns 0 or 1 for flag value in data object being written to db"""
    
    
    keys_to_remove = ['update', 'correction', 'data_version', 'release_version']
    modified_update_dict = {}
    modified_previous_version_dict = {}
    
    for key, value in data.items():
        if not any( keys in key for keys in keys_to_remove ):
            modified_update_dict[ key ] = value
    try:
        for key, value in update_latest_dict[ subject_id ].items():
            if not any( keys in key for keys in keys_to_remove ):                
                modified_previous_version_dict[key] = value

    except:
        print("This subject is not in the current published release.  Update_latest flag set to 1")
        return 1

    for key, value in modified_update_dict.items():

        if str( value ) == str( modified_previous_version_dict[ key ] ):
            continue
        else:
            print(f' { subject_id }: difference between new record and previous version found for {key}')
            return 1
    
    return 0

## update adstatus
def build_adstatus_check_dict( subject_type ):
    _baseline_ad_data = database_connection(f"SELECT subject_id, _baseline_data->>'ad' \
        FROM ds_subjects_phenotypes_baseline WHERE subject_type = '{ subject_type }'")

    adstatus_check_dict = { record[ 0 ]:  record[ 1 ] for record in _baseline_ad_data }

    return adstatus_check_dict

def update_adstatus_check( subject_id, ad_status, adstatus_check_dict ):
    """takes subject_id, subject_type, and data to be written to database, 
    checks ad value for baseline version, returns appropriate value for new data for adstatus flag"""
    

    if subject_id in adstatus_check_dict:
        if str( ad_status ) == adstatus_check_dict[ subject_id ]:
            return 0
        else:
            return 1
    else:
        print(f'{ subject_id } has no previously published AD status, so 0 will be returned')
        return 0

## update correction
def correction_check( data ):
    """takes data about to be written to database, 
    checks if comments field has word 'corrected' in it and returns appropriate boolean value"""
    for key, value in data.items():
        if key.lower() == "comments":
            if 'corrected' in str( value ).lower():
                return 1
            else:
                return 0 

## update diagnosis
def build_update_diagnosis_check_dict( subject_type ):
    """takes subject type as arg, returns dict keyed by subject_id of appropriate diagnosis update variables to compare, depending on subject_type"""
    if subject_type == 'ADNI':
        query_variables = [ 'ad_last_visit', 'mci_last_visit' ]
    
    if subject_type == 'PSP/CDB':
        query_variables = [ 'diagnosis' ]

    query = database_connection(f"SELECT subject_id, _baseline_data \
        FROM ds_subjects_phenotypes_baseline WHERE subject_type = '{ subject_type }'")
    
    _baseline_diagnosis_data = { record[ 0 ]: record[ 1 ] for record in query }

    diagnosis_update_check_dict = {}
    for subject_id, data in _baseline_diagnosis_data.items():
        diagnosis_data = {}
        for key, value in data.items():
            ## needed to get the json so have the variable keys, but only want the ones needed for diagnosis update check, so those only get added to a new dict
            if key in query_variables:
                diagnosis_data[ key ] = value
        
        diagnosis_update_check_dict[ subject_id ] = diagnosis_data

    return diagnosis_update_check_dict

def update_diagnosis_check( subject_id, subject_type, data, diagnosis_update_check_dict ):
    """takes subject_id, subject_type, data to be written to database, and diagnosis_check_dict (generated based on subject_type),
    checks diagnosis value (eg. for adni data) for baseline version, returns appropriate value for new data for diagnosis_update flag"""
    
    
    try:
        subject_diagnosis_dict = diagnosis_update_check_dict[ subject_id ]
    except KeyError:
        print( f'{ subject_id } has no previous diagnosis value(s).' )
        return 0

    if subject_type == 'ADNI':
        try:
            baseline_ad = subject_diagnosis_dict[ "ad_last_visit" ]
            baseline_mci = subject_diagnosis_dict[ "mci_last_visit" ]
        except:
            print(f'{ subject_id }: no value found for baseline_ad or baseline_mci.')
            return 0
        
        if data[ "ad_last_visit" ] == baseline_ad and data[ "mci_last_visit" ] == baseline_mci:
            return 0
        else:
            return 1
    
    if subject_type == 'PSP/CDB':
        try:
            diagnosis = subject_diagnosis_dict[ "diagnosis" ]

        except:
            print( f'{ subject_type }: no value found for diagnosis.' )
            return 0
        
        if data[ "diagnosis" ] == diagnosis:
            return 0
        else:
            return 1