import sys
sys.path.append('/id_db/.venv/lib/python3.6/site-packages')

import psycopg2
import csv
from dotenv import load_dotenv
import os
import json
import datetime

import pandas as pd

from pheno_utils import *

def main():
    user_input_subject_type = get_subject_type()

    LOADFILE = get_filename()

    data_dict = create_comparison_data_dict( LOADFILE, user_input_subject_type )
    dict_name = database_connection(f"SELECT dictionary_name FROM env_var_by_subject_type WHERE subject_type = '{ user_input_subject_type }'")[ 0 ][ 0 ]
    dictionary = get_dict_data( dict_name )


if __name__ == '__main__':
    print('start ', datetime.datetime.now().strftime("%H:%M:%S") )
    main()  
    print('end ', datetime.datetime.now().strftime("%H:%M:%S") )