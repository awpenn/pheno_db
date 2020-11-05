import sys
sys.path.append('/id_db/.venv/lib/python3.6/site-packages')

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
    """takes dict_name as arg, and..."""
    dict_data = [ return_tuple[ 0 ] for return_tuple in database_connection(f"SELECT _dict_data FROM data_dictionaries WHERE dictionary_name = '{ dict_name }'") ][ 0 ]
    ## create dataframe from returned json, with keys as rows
    data_df = pd.DataFrame.from_dict( dict_data, orient='index' )

    ## add the index (varname) as column in df
    data_df[ 'variable' ] = data_df.index

    ## save the dictionary name (to create filename) before deleting column
    dictionary_name = set( data_df[ 'dictionary_name' ] ).pop()

    ## create dictname column from df
    del data_df[ 'dictionary_name' ]

    ## re-order the columns in df
    data_df = data_df[ [ 'variable', 'variable_description', 'data_values', 'comments' ] ]

    ## write df as tab-separated file
    data_df.to_csv( f'{ dictionary_name }_dict.txt', sep="\t", index=False )


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
