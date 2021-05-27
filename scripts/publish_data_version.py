"""
Publish a data release without uploading any changes
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

script_name = 'publish_data_version.py'

def main( ):

    ## User select datatype
    user_input_subject_type = pheno_utils.get_subject_type( )

    ## Select release to publish from a list (filted by published = false)
    name_of_release_to_publish, tablekey_of_release_to_publish = pheno_utils.user_input_data_version_to_publish( )
    ## update published status for subjects
    ## add baseline records for new subjects
    ## change version publish status
    pheno_utils.publish_subjects_and_data_version( data_version_published_status = 'TRUE', data_version = tablekey_of_release_to_publish, subject_type = user_input_subject_type )
    
if __name__ == '__main__':
    main( )
