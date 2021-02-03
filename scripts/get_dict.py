import sys
sys.path.append('/pheno_db/.venv/lib/python3.7/site-packages/')

import psycopg2
import csv
from dotenv import load_dotenv
import os
import json

import pandas as pd

import calendar
import time

from flagchecks import *
from pheno_utils import *

def main():
    dict_name = user_select_dictionary()
    make_dictionary_csv( dict_name )

def make_dictionary_csv( dict_name ):
    """takes dict_name as arg, calls data retrieve function, writes csv"""
    data_df = get_dict_data( dict_name )

    ## write df as tab-separated file
    data_df.to_csv( f'{ dict_name }_dict.txt', sep="\t", index=False )


def user_select_dictionary():
    """no args, returns choice of dictionary"""
    ## make a list from the tuples returned from query for dictionary_names in the table
    dictionaries = [ name_tuple[ 0 ] for name_tuple in database_connection("SELECT DISTINCT dictionary_name FROM data_dictionaries") ]

    while True:
        try:
            selection_input = input(f"Select dictionary from list { dictionaries }")
        except ValueError:
            continue
        if selection_input in dictionaries:
            return selection_input
        else:
            print("Please select a name from the list. ")
            continue

if __name__ == '__main__':
    main()
