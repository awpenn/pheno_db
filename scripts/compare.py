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

def main():
    """main conductor function for the script."""
    user_input_subject_type = get_subject_type()
    
    ## calls function that returns the three view names based on user_input
    views_based_on_subject_type = get_views_by_subject_type( user_input_subject_type )

    query_type = get_compare_query_type()

    data_from_db = get_data( query_type, views_based_on_subject_type )

    sorted_df = build_dataframe( query_type, views_based_on_subject_type, data_from_db )

    sorted_df_with_highlights = highlight_change( query_type, sorted_df )

    build_comparison_table( user_input_subject_type, query_type, sorted_df_with_highlights )

def get_data( query_type, views_based_on_subject_type ):
    """takes query_type (if update/latest or update/baseline) and views based on subject type as args, and creates comparison dict and list of headers"""
    current_view, update_view, baseline_view = views_based_on_subject_type

    if query_type == 'update_to_latest':
        data = database_connection(f"SELECT * FROM {update_view} LEFT JOIN {current_view} ON {update_view}.subject_id = {current_view}.subject_id")
        header_data = database_connection(f"SELECT column_name FROM information_schema.columns WHERE table_name in('{update_view}' ,'{current_view}');" )
        headers = [ ''.join(header) for header in header_data ]

    if query_type == 'update_to_baseline': 
        ##need to get update_ tracking variables first and add to the query
        tracking_columns = get_latest_published_tracking_varnames( current_view )
        ## join them as a string to be inserted into the query
        tracking_columns_query_string = ', '.join( tracking_columns )
        data = database_connection(f"SELECT {update_view}.*, {baseline_view}.*, {tracking_columns_query_string} \
                                    FROM {update_view} \
                                    LEFT JOIN {baseline_view} ON {update_view}.subject_id = {baseline_view}.subject_id \
                                    LEFT JOIN {current_view} ON {update_view}.subject_id = {current_view}.subject_id \
                                  ")
        #n.b. query below is hacky, figure out how to do without the sorting based on table name and ord                                              
        header_data = database_connection(f"SELECT column_name FROM information_schema.columns WHERE table_name in('{update_view}' ,'{baseline_view}') ORDER BY table_name DESC, ordinal_position;;" )
        headers = [ ''.join(header) for header in header_data ]
        ## split the table reference off each tracking var, and preprend latest_pub to it, then APPEND to headers list
        [ headers.append( ''.join(( 'LATEST_PUB_', var[ var.index( "." ) + 1: ] )) ) for var in tracking_columns ]

    return headers, data

def build_dataframe( query_type, views_based_on_subject_type, header_and_data_db_responses ):
    """takes query_type, views, the _data and headers responses from get_data as args, returns appropriately constructed comparison dataframe"""
    headers_unpacked, data = header_and_data_db_responses
    skip_column_keywords = [ 'update', 'published', 'comment', 'correction' ]
    unique_headers_len = int( len( headers_unpacked )/2 )
    headers_cleaned = []
    headers_sorted = []


    if query_type == 'update_to_latest':
        """for update to latest, need to have _baseline added to differentiate between the current and update cols"""
        for index, header in enumerate( headers_unpacked ):
            """takes headers, gives the 'current' ones a 'baseline_' appendage"""
            if index >= unique_headers_len:
                headers_cleaned.append( f"prev_{header}" )
            else:
                headers_cleaned.append( header )

        for index, header in enumerate( headers_cleaned ):
            """orders cleaned headers so like columns are next to eachother (may discard)"""
            if index <= unique_headers_len:
                if header not in ['subject_id', 'prev_subject_id']:
                    headers_sorted.append( header )
                    try:
                        headers_sorted.append( headers_cleaned[ headers_cleaned.index( f"prev_{header}" ) ] )
                    except:
                        continue
                elif header != 'prev_subject_id':
                    headers_sorted.append( header )
    
    if query_type == 'update_to_baseline':
        for index, header in enumerate( headers_unpacked ):
            if header != 'subject_id':
                headers_cleaned.append( header )
            elif header in headers_cleaned:
                headers_cleaned.append( f"DISCARD_subject_id" )
            else:
                headers_cleaned.append( header )

        for index, header in enumerate( headers_cleaned ):
            """orders headers so like columns are next to eachother (may discard)"""
            # cant go on "unique header len" because the baseline view is different than subject view
            if header not in headers_sorted:
                if 'baseline_' not in header: #hack way to eliminate the extra subject_id col
                    if header != 'DISCARD_subject_id':
                        headers_sorted.append( header )
                        try:
                            headers_sorted.append( headers_cleaned[ headers_cleaned.index( f"baseline_{header}" ) ] )
                        except:
                            continue

    unsorted_df = pd.DataFrame( data, columns=[ headers_cleaned ] )
    sorted_df = unsorted_df[ headers_sorted ]
    # convert column names to strings (from tuples)
    sorted_df.columns = [str(i[0]) for i in sorted_df.columns]

    ## delete the columns for update versions tracking vars
    for column in sorted_df:
        if "prev_" not in column and "LATEST_PUB" not in column and any( k in column for k in skip_column_keywords ):
                del sorted_df[ column ]


    return sorted_df

def highlight_change( query_type, sorted_dataframe ):
    """takes query_type and comparison dataframe, checks if there are differences between update and latest/baseline, 
    adds 'X.value TO Y.value' to end of row
    """
    if query_type == 'update_to_latest':
        compare_prefix = 'prev'
    if query_type == 'update_to_baseline':
        compare_prefix = 'baseline'

    def add_update(var_update,var_y,var_x):
        comp_update = sorted_dataframe[var_y].apply(str) + " to " + sorted_dataframe[var_x].apply(str)
        comp_update[sorted_dataframe[var_y] == sorted_dataframe[var_x]] = ""

        sorted_dataframe[var_update] = comp_update
    ## access columns eg. sorted_dataframe[['sex', 'prev_sex']] //note twin brackets
    skip_column_keywords = ['update', 'published', 'release', 'version']

    for j in sorted_dataframe.columns:
        ## if no part of any keyword appears in the current column name (j)
        if not any(k in j for k in skip_column_keywords):
            if f"{compare_prefix}_{j}" in sorted_dataframe:
                add_update(f"{j}_change", f"{compare_prefix}_{j}", j)

    return sorted_dataframe

def build_comparison_table( subject_type, query_type, comparison_dataframe ):
    """takes query_type for filename and finished dataframe and creates csv"""
    ## need to remove any `/` in subject_type for placement in filename
    subject_type_corrected = subject_type.replace( "/", "-" )
    comparison_dataframe.to_csv(f"./comparison_files/{ subject_type_corrected }_{ query_type }_comparison.csv",index=False,na_rep="NA")

def get_latest_published_tracking_varnames( current_view ):
    """takes current_view and returns latest published tracking variables list to be added to query"""
    ## first get column names from view with _update, because differnet for ADNI
    column_names = database_connection(f"SELECT column_name \
                        FROM information_schema.columns \
                        WHERE table_name = '{ current_view }' \
                        and column_name like 'update_%' \
                    ")

    tracking_vars = [ f"{current_view}.{str( var[ 0 ] )}" for var in column_names ]
    tracking_vars.append(f"{current_view}.comments")
    tracking_vars.append(f"{current_view}.correction")

    return tracking_vars

if __name__ == '__main__':
    main()
    # generate_errorlog()
    # generate_success_list()
