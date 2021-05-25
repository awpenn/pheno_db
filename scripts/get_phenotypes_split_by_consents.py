"""
Like the phenotypes by consent level in consent_database, 
this will pull latest published phenotypes for a subject_type, get consent level by matching 
adspid( phenodb ) to site_indiv_id ( consentdb ) via adspid_db, build a dict of phenotype records keyed by 
consent level, then build output files split by consent level
"""
import sys
sys.path.append('/home/pheno_db/.venv/lib/python3.6/site-packages/')

import csv
import os
import json
import datetime
import pandas as pd

import pheno_utils
script_name = 'get_phenotypes_split_by_consents.py'


def main( ):
    database_type = pheno_utils.select_database_type( )

    user_input_subject_type = pheno_utils.get_subject_type( )

    pheno_dict, by_consents_dict = pheno_utils.get_phenotype_and_consent_level_data( database_type = database_type, subject_type = user_input_subject_type )
    
    build_output_files_split_by_consent( subjects_dict = by_consents_dict, subject_type = user_input_subject_type )

##script-specific functions
def build_output_files_split_by_consent( subjects_dict, subject_type ):
    """takes dict of subjects, creates dataframe and csv file with phenotypes and subjects consent/cohort"""
    subjects = subjects_dict

    print( f'{ len( subjects.keys( ) ) } consent level(s) found for { subject_type } subjects...\n' )

    for consent, subjects in subjects.items( ):
        print( f'Creating output file for { consent }...' )
        if len( subjects ) > 1:
            df = pd.read_json( json.dumps( subjects ) ).transpose( )

        else: ##if only one subject in df, need to do this reindex thing to preserve column order
            df = pd.read_json( json.dumps( subjects ) ).transpose( )
            df = df.reindex(columns=list( subjects[ list( subjects.keys( ) )[ 0 ] ].keys( ) ) ) ##take subject dict, get keys as list, use first (only) key to get subject's keys (ie. the columns)
        
        ## drop the site_id from final file, was only used to match
        df = df.drop( 'site_indiv_id', axis=1 )

        date = datetime.date.today( )
        time = datetime.datetime.now( ).strftime("%H-%M-%S")

        ##need to remove slash in case/control for filename
        if subject_type == 'case/control':
            subject_type = 'case-control'

        df.to_csv(f"./log_files/{ subject_type }-{ consent }-phenotypes-and-consents-{ date }-{ time }.txt", sep="\t", index=False )

        print( f'Output file " { subject_type }-{ consent }-phenotypes-and-consents-{ date }-{ time }.txt " in log_files directory.\n' )

if __name__ == '__main__':
    main( )