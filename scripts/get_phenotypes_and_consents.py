"""
Pulls latest published phenotypes for a subject_type, get consent level by matching 
adspid( phenodb ) to site_indiv_id ( consentdb ) via adspid_db, creates csv with phenotypes data and cohort/consent level
"""
import sys
sys.path.append('/home/pheno_db/.venv/lib/python3.6/site-packages/')

import csv
import os
import json
import pandas as pd

import pheno_utils
script_name = 'get_phenotypes_and_consents.py'


def main( ):
    database_type = pheno_utils.select_database_type( )

    user_input_subject_type = pheno_utils.get_subject_type( )

    ## get file prefix from user
    user_input_filename_prefix = pheno_utils.get_filename_prefix( )

    pheno_dict, by_consents_dict = pheno_utils.get_phenotype_and_consent_level_data( database_type = database_type, subject_type = user_input_subject_type )
    
    if by_consents_dict:
        build_output_file( subjects_dict = pheno_dict, subject_type = user_input_subject_type, filename_prefix = user_input_filename_prefix )
    else:
        print( f'No published {  user_input_subject_type } records found with consent levels.' )

##script-specific functions
def build_output_file( subjects_dict, subject_type, filename_prefix ):
    """takes dict of subjects, creates dataframe and csv file with phenotypes and subjects consent/cohort"""
    subjects = subjects_dict

    print( 'Creating output file...' )
    if len( subjects ) > 1:
        df = pd.read_json( json.dumps( subjects ) ).transpose( )

    elif len( subjects ) == 1: ##if only one subject in df, need to do this reindex thing to preserve column order
        df = pd.read_json( json.dumps( subjects ) ).transpose( )
        df = df.reindex(columns=list( subjects[ list( subjects.keys( ) )[ 0 ] ].keys( ) ) ) ##take subject dict, get keys as list, use first (only) key to get subject's keys (ie. the columns)
    
    ## drop the site_id from final file, was only used to match
    df = df.drop( 'site_indiv_id', axis=1 )

    ##need to remove slash in case/control for filename
    if subject_type == 'case/control':
        subject_type = 'case-control'

        df.to_csv( f"./log_files/{ filename_prefix }-ALL.txt", sep="\t", index=False )

        print( f'Output file " { filename_prefix }-ALL.txt " in log_files directory.\n' )
if __name__ == '__main__':
    main( )