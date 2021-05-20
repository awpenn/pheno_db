import sys
sys.path.append('/home/pheno_db/.venv/lib/python3.6/site-packages/')

import psycopg2
import csv
from dotenv import load_dotenv
import os
import json

import calendar
import time

import pheno_utils

def main():
    """main conductor function for the script.  Takes some input about the type of data being uploaded and runs the process from there."""

    user_input_subject_type = pheno_utils.get_subject_type()

    views_based_on_subject_type = pheno_utils.get_views_by_subject_type( user_input_subject_type )

    for view in views_based_on_subject_type:
        if 'unpublished' in view:
            view_based_on_subject_type = view

    is_batch_file = pheno_utils.user_input_batch_loading()

    if is_batch_file:
       LOADFILE = pheno_utils.get_filename()
       drop_dict = create_drop_data_dict( LOADFILE, view_based_on_subject_type )

    else:
       drop_dict = pheno_utils.get_subject_to_drop( view_based_on_subject_type )
    
    # drop_dict returns dict keyed by subject id with data_version (pkey from table) as value
    drop_from_database( user_input_subject_type, drop_dict )

    pheno_utils.generate_errorlog( )
    
def create_drop_data_dict( LOADFILE, view_based_on_subject_type ):
    """takes loadfile name and view to look up based on subject_type as args, 
    returns dict of json data keyed by subject id of data to be entered in database"""
    drop_dict = {}

    with open( f'./source_files/{ LOADFILE }', mode='r', encoding='utf-8-sig' ) as csv_file:
        pheno_file = csv.reader( csv_file )
        headers = next( pheno_file )
        
        for index, row in enumerate( pheno_file ): 
            subjid = row[ headers.index( "subjid" ) ] or None
            release_version = row[ headers.index( "release_version" ) ] or None
            data_version_id = pheno_utils.get_data_version_id( release_version ) or None
            
            missing_data = True in ( ele == None for ele in [ subjid, release_version, data_version_id ] )

            if missing_data:
                err = f'There are empty fields and/or data errors in the the loadfile, row { index + 1 }'
                err_obj = {
                    "subjid": subjid,
                    "release_version": release_version,
                    "version_id_returned": data_version_id
                }
                print( err )

                pheno_utils.error_log[ len( pheno_utils.error_log ) + 1 ] = [ err, err_obj ]
                continue
            else:
                if pheno_file.line_num > 1:
                    if pheno_utils.check_subject_exists( view_based_on_subject_type, subjid, release_version ):
                        try:
                            if isinstance( data_version_id, int ):
                                drop_dict[ subjid ] = data_version_id
                        except:
                            err = f"release_version ({ release_version }) given for { subjid } is not in database.  Subject drop discarded.  Check data."
                            print( err )
                            pheno_utils.error_log[ len( pheno_utils.error_log ) + 1 ] = [ err ]
                    else:
                        err = f' Subject { subjid } has no unpublished record in { row[ headers.index( "release_version" ) ] }.  Subject drop discarded. Check data.'
                        print( err )
                        pheno_utils.error_log[ len( pheno_utils.error_log ) + 1 ] = [ err ]
              
    return drop_dict

def drop_from_database( subject_type, drop_dict ):
    """takes subject_type and dict keyed by subject id, with id for data version in which subject's record will be deleted."""
    for key, value in drop_dict.items():
        try:
            pheno_utils.database_connection( f"DELETE FROM ds_subjects_phenotypes WHERE subject_id = %s AND _data->>'data_version' = '%s' AND subject_type = %s AND published = FALSE", ( key, value, subject_type )  )
        except:
            err = f'Failed to drop subject { key } from { value }'
            pheno_utils.error_log[ len( pheno_utils.error_log ) + 1 ] = [ err ]


if __name__ == '__main__':
    main()