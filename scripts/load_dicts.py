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
    """main conductor function for the script.  Takes some input about the type of data being uploaded and runs the process from there."""
    LOADFILE = get_filename()
    data_dict = create_data_dict( LOADFILE )

    cleanup_string_numbers( data_dict )

    write_to_db( data_dict )

def create_data_dict( LOADFILE ):
    """takes loadfile name as arg, returns dict of json data keyed by subject id of data to be entered in database"""
    data_dict = {}

    upload_data = pd.read_csv(f"./source_files/{LOADFILE}", dtype=str)
    # tada = upload_data.loc[upload_data['VARNAME'] == 'APOE']
    # turn the values into a list val_list = [x for x in tada['VALUES]]

    ## use list comprehension to create set of variables from dataframe, then use list comprehension to convert set to list
    unique_variables = [ u_var for u_var in set( [ var for var in upload_data.VARNAME ] ) ]
    
    for var in unique_variables:
        variable_data_object = {}
        value_dict = {} ## this is the nested dict that will hold data values if required

        ## build list of the values found in the dataframe for the variable given in the loop of unique_variables
        value_list = [ value for value in upload_data.loc[upload_data[ 'VARNAME' ] == var ][ 'VALUES' ] ]

        ## get the value-pairs from the value_list, split by colon, and assign as key-value pair to value_dict
        for value in value_list:
            if not pd.isnull(value):
                key_and_value = value.split(':')
                value_dict[ key_and_value[ 0 ] ] = key_and_value[1]
        
        ## write dict of values and keys to the variable's data object
        variable_data_object[ 'data_values' ] = cleanup_string_numbers( value_dict )

        ## get the DICTNAME based on the current var in parent loop, first build list, then exclude NaN
        dictname_list = [ name for name in upload_data.loc[ upload_data[ 'VARNAME' ] == var ][ 'DICTNAME' ] ]
        ## if any of the names are different than the first, alert, else continue
        if not any( pd.isnull( name ) for name in dictname_list ): 
            if not any( name != dictname_list[0] for name in dictname_list ):
                ## set dict_name for variable object to the first in the name list
                variable_data_object [ 'dictionary_name' ] = dictname_list[ 0 ]
            else:
                print( f'There are multiple dictionary names associated with {var}, please choose the correct one' )
                variable_data_object[ 'dictionary_name' ] = get_user_input( dictname_list )
        
        ## gather the descriptions, drop the nans, if are more than one/differing descriptions, get user input for value
        description_list = [ desc for desc in upload_data.loc[ upload_data[ 'VARNAME' ] == var ][ 'VARDESC' ].dropna() ]
        ## if any or the descriptions returned differ (in essense, if there's accidently more than one associated with a variable)
        if not any( desc != description_list[0] for desc in description_list ): 
            ## set description for variable object to the first in the desc list
            variable_data_object [ 'variable_description' ] = description_list[ 0 ]
        else:
            print( f'There are multiple descriptions associated with {var}, please choose the correct one' )
            variable_data_object[ 'dictionary_description' ] = get_user_input( description_list )
        
        ## gather comments, drop nans, if are more than one/differing descriptions, get user input for value
        comments_list = [ desc for desc in upload_data.loc[ upload_data[ 'VARNAME' ] == var ][ 'COMMENTS' ].dropna() ]
        
        ## if there are any comments
        if comments_list:
            if not any( desc != comments_list[0] for desc in comments_list ): 
                ## set comments for variable object to the first in the comments list
                variable_data_object [ 'comments' ] = comments_list[ 0 ]
            else:
                print( f'There are multiple comments associated with {var}, please choose the correct one' )
                variable_data_object[ 'comments' ] = get_user_input( comments_list )
        else:
                variable_data_object[ 'comments' ] = None

        ## now all data assembled, add it to the parent dict, keyed by varname
        data_dict[ var.lower() ] = variable_data_object
     
    return data_dict

def write_to_db( data_dict ):
    """writes record to db (for now writing to a dict.json to check correctness)"""
    dictionary_name = get_dictionary_name( data_dict )
    ## .replace to replace the single with two-single, allows to write to db, comes back as should be written
    _dict_data = json.dumps( data_dict ).replace("'", "''")

    database_connection(f"INSERT INTO data_dictionaries(dictionary_name, _dict_data) VALUES('{ dictionary_name }', '{ _dict_data }');")

    with open('dict.json', 'w') as f:
        json.dump( data_dict, f )

def get_user_input( value_list ):  
    """takes the list of values pulled from dataframe in cases where there is more than one,
     (e.g. same variable has two assoc. dictionary names), gets user to choose correct one and returns"""
    while True:
        try:
            value_input = input(f"Which value is correct? {set(value_list)}")
        except ValueError:
            continue
        if str(value_input) in value_list:
            return value_input
        else:
            print(f"{value_input} is not valid.  Please select a value from the list")
            continue

def cleanup_string_numbers( value_dict ):
    """takes the values dict from builder function, looks where it can turn strings back into numbers"""
    cleaned_up_dict = {}

    for key, value in value_dict.items():
        try:
            cleaned_up_dict[ key ] = int( value )
        except:
            cleaned_up_dict[ key ] = value

    return cleaned_up_dict

def get_dictionary_name( data_dict ):
    """checks that all the var entries have same dictionary name, 
    returns if does, else alerts the user that there is error in data and ends program"""
    dictionary_name_list = []
    for key, value in data_dict.items():
        for dkey, dvalue in value.items():
            if dkey == 'dictionary_name':
                dictionary_name_list.append( dvalue )
    
    if any( dictionary_name_list[ 0 ] != dname for dname in dictionary_name_list ):
        print(f" There is more than dictionary_name found in the uploaded dict { set( dictionary_name_list ) }.  Check upload file and retry.  Program will exit. ")
        sys.exit()
    else:
        return dictionary_name_list[ 0 ]


if __name__ == '__main__':
    main()
