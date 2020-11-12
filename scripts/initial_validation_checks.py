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
    dictionary = get_dict_data( subject_type )
    breakpoint()

if __name__ == '__main__':
     ## for testing, will read from dict.json file instead of normal function workflow, remove after
    data_dict = get_test_json_from_file( 'data.json' ) ##this is case/control data from `starting_data_for_test` file
    validation_checks_overlord( 'case/control', data_dict )
