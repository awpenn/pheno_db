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

def main():
    """main conductor function for the script."""
    user_input_subject_type = get_subject_type()
    
    if user_input_subject_type == 'case/control':
        views_based_on_subject_type = 'get_current_cc', 'get_unpublished_updates_cc'
    if user_input_subject_type == 'family':
        views_based_on_subject_type = 'get_current_fam', 'get_unpublished_updates_fam'

    comparison_dict_and_headers = create_comparison_dict( views_based_on_subject_type )

    build_comparison_table( comparison_dict_and_headers )

def create_comparison_dict( views_based_on_subject_type ):
    """takes views based on subject type as arg, and creates comparison dict and list of headers"""
    compare_dict = {}
    headers_list = []
    current_view, update_view = views_based_on_subject_type

    data = database_connection(f"SELECT * FROM {update_view} LEFT JOIN {current_view} ON {update_view}.subject_id = {current_view}.subject_id")
    # headers = database_connection(f"SELECT table_name, ordinal_position, column_name FROM information_schema.columns WHERE table_name in('{update_view}' ,'{current_view}');" )
    header_data = database_connection(f"SELECT column_name FROM information_schema.columns WHERE table_name in('{update_view}' ,'{current_view}');" )
    
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

    for index, header in enumerate( headers_cleaned ):
        """orders cleaned headers so like columns are next to eachother (may discard)"""
        if index < unique_headers_len:
            headers_sorted.append( header )
            headers_sorted.append( headers_cleaned[ headers_cleaned.index( f"baseline_{header}" ) ] )
    
    #this unsorted_df is same as the merged tables in JM code
    unsorted_df = pd.DataFrame(data, columns=[headers_cleaned])
    #this is same frame but with like columns next to eachother
    sorted_df = unsorted_df[headers_sorted]

    breakpoint()
    

    for p_value in data:
        subject_id = p_value[ 0 ]
        subject_dict = {}

        for index, h_value in enumerate(headers):
            if index <= unique_headers_len-1:

                phenotype = h_value[ 1 ]
                headers_list.append( phenotype )
                update_val = p_value[ index ]
                current_val = p_value[ index + unique_headers_len]
                if update_val != current_val:
                    values = f"{update_val}, {current_val}"
                else:
                    values = ""
                subject_dict[phenotype] = values

        subject_dict.pop("subject_id")
        compare_dict[subject_id] = subject_dict

    return compare_dict, headers_list

def build_comparison_table(comparison_dict_and_headers):
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



    