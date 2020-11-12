import sys
sys.path.append('/id_db/.venv/lib/python3.6/site-packages')

import psycopg2
import csv
from dotenv import load_dotenv
import os
import json

import pandas as pd

from pheno_utils import *

### named 'a' for running tests, will be validation_checks_overlord( subject_type, data_dict )
# def a():
def validation_checks_overlord( subject_type, data_dict ):
    """takes the subject_type and data_dict from pheno handling scripts, calls series of validation checks and returns ???"""
    
    dict_name = database_connection(f"SELECT dictionary_name FROM env_var_by_subject_type WHERE subject_type = '{ subject_type }'")[ 0 ][ 0 ]
    dictionary = get_dict_data( dict_name )

    run_checks( data_dict, dictionary )
    breakpoint()

def run_checks( data_dict, dictionary ):
    """takes data_dict and dictionary as args, checks that all values in data are valid, returns ???"""
if __name__ == '__main__':
     ## for testing, will read from dict.json file instead of normal function workflow, remove after
    data_dict = get_test_json_from_file( 'data.json' ) ##this is case/control data from `starting_data_for_test` file
    validation_checks_overlord( 'case/control', data_dict )
