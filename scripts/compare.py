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
 
from pheno_utils import *

new_records = []
success_id_log = []
error_log = {}

compare_family_data = ''
query_type = ''

def main():
    """main conductor function for the script."""
    user_input_subject_type = get_subject_type()
    
    if user_input_subject_type == 'case/control':
        views_based_on_subject_type = 'get_current_cc', 'get_unpublished_updates_cc'
    if user_input_subject_type == 'family':
        views_based_on_subject_type = 'get_current_fam', 'get_unpublished_updates_fam'

    query_type = get_compare_query_type()

    data_from_db = get_data( query_type, views_based_on_subject_type )

    df = build_dataframe( query_type, data_from_db )
    breakpoint()

def get_data( query_type, views_based_on_subject_type ):
    """takes query_type (if update/latest or update/baseline) and views based on subject type as args, and creates comparison dict and list of headers"""
    current_view, update_view = views_based_on_subject_type

    if query_type == 'update_to_latest':
        data = database_connection(f"SELECT * FROM {update_view} LEFT JOIN {current_view} ON {update_view}.subject_id = {current_view}.subject_id")
        header_data = database_connection(f"SELECT column_name FROM information_schema.columns WHERE table_name in('{update_view}' ,'{current_view}');" )
    
    if query_type == 'update_to_baseline': # need to fix this, needs to pull unpacked baseline data from view that doesnt exist yet
        data = database_connection(f"SELECT update_view.*,  FROM {update_view} LEFT JOIN ds_subjects_phenotypes_baseline ON {update_view}.subject_id = ds_subjects_phenotypes_baseline.subject_id")
        header_data = database_connection(f"SELECT column_name FROM information_schema.columns WHERE table_name in('{update_view}' ,'ds_subjects_phenotypes_baseline');" )
        
    return header_data, data

def build_dataframe( query_type, header_and_data_db_responses ):
    """takes query_type, the _data and headers responses from get_data as args, returns appropriately constructed comparison dataframe"""
    header_data, data = header_and_data_db_responses

    headers_unpacked = [''.join(header) for header in header_data] 
    unique_headers_len = int( len( headers_unpacked )/2 )
    headers_cleaned = []
    headers_sorted = []

    for index, header in enumerate( headers_unpacked ):
        """takes headers, gives the 'current' ones a 'baseline_' appendage"""
        if index >= unique_headers_len:
            headers_cleaned.append( f"baseline_{header}" )
        else:
            headers_cleaned.append( header )
    breakpoint()
    for index, header in enumerate( headers_cleaned ):
        """orders cleaned headers so like columns are next to eachother (may discard)"""
        if index < unique_headers_len:
            headers_sorted.append( header )
            headers_sorted.append( headers_cleaned[ headers_cleaned.index( f"baseline_{header}" ) ] )
    
    #this unsorted_df is same as the merged tables in JM code
    unsorted_df = pd.DataFrame(data, columns=[headers_cleaned])
    #this is same frame but with like columns next to eachother
    sorted_df = unsorted_df[headers_sorted]

    return sorted_df

def build_comparison_table( comparison_dataframe ):
    """takes comparison dict and list of headers, creates table for comparison"""
    comparison_dict = comparison_dict_and_headers[ 0 ]
    headers = comparison_dict_and_headers[ 1 ]

    with open('tada.csv', mode='w') as csv_file:
        fieldnames = headers
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        for subject_id, data in comparison_dict.items():
            data["subject_id"] = subject_id
            writer.writerow(data)

if __name__ == '__main__':
    main()
    # generate_errorlog()
    # generate_success_list()



    