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

import flagchecks
import pheno_utils

def main( ):
    database_type = select_database_type( )
    ## 1. get subject type from user


    user_input_subject_type = pheno_utils.get_subject_type( )

    LOADFILE = pheno_utils.get_filename( )

    ## 2. build dict of phenotype records based on subject_type ( current view )
    ## 3. build dict of data from adspid database ( site_indiv_id, adspid, cohort_code ) ( limited by subject_type? )
    ##      a. add site_indiv_id and cohort code to phenotype dict ( by matching adspids )
    ## 4. build dict from subjects_cohorts_consents ( cdb ) data
    ##      a. using site_indiv_id, add resolved_consent to phenotype dict ( by matching site_indiv_id + cohort )
    ## 5. build a set of resolved consents found in previous steps
    ## 6. build dict, keyed by consent level, each entry = a nested dict containing sorted phenotype records
    ## 7. for each entry in #6 dict, create a csv with phenotype records for that consent level

##script-specific functions
def select_database_type( ):
    """takes no arguments, returns value for DB attribute in database connection, so user can select sandbox or production"""
    while True:
        try:
            db_input = input(f"Do you want to load data to the sandbox or production database? (Select number). 1: sandbox, 2: production ")
        except ValueError:
            continue
        
        if not db_input:
            continue

        elif not db_input.isdigit():
            continue

        elif db_input not in ['1', '2']:
            print('Please select an option from the list.')
            continue

        else:
            if int( db_input ) == 1:
                return 'SANDBOX'
            else:
                confirm_input = input( f"You have selected the production database. Please confirm.(y/n) " )
                valid_confirm_input = [ 'y', 'n' ]
                if confirm_input.lower() not in valid_confirm_input:
                    continue
                else:
                    if confirm_input.lower() == 'y':
                        return 'production'        
                    else:
                        continue
                    
if __name__ == '__main__':
    main( )