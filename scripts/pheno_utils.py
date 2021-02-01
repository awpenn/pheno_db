import sys
sys.path.append('/id_db/.venv/lib/python3.6/site-packages')

import psycopg2
import csv
from dotenv import load_dotenv
import os
import json
import datetime

import pandas as pd

error_log = {}

load_dotenv()
DBIP = os.getenv('DBIP')
DBPASS = os.getenv('DBPASS')
DBPORT = os.getenv('DBPORT')
DB = os.getenv('DB')
DBUSER = os.getenv('DBUSER')

def database_connection(query):
    """takes a string SQL statement as input, and depending on the type of statement either performs an insert or returns data from the database"""

    try:
        connection = psycopg2.connect(user = DBUSER, password = DBPASS, host = DBIP, port = DBPORT, database = DB)
        cursor = connection.cursor()
        cursor.execute(query)

        if "SELECT" in query:
            returned_array = cursor.fetchall()
            cursor.close()
            connection.close()
            
            return returned_array

        else:
            connection.commit()
            cursor.close()
            connection.close()
            

    except (Exception, psycopg2.Error) as error:
        print('Error in database connection', error)
    

    finally:
        if(connection):
            cursor.close()
            connection.close()
            # print('database connection closed')

#check-functions for data correctness
def build_dupecheck_list( data_version_id, pub_check, subject_type ):
    """takes version, publication status and subject type as args,
    returns list of subjects for a particular release version to check for duplicates rather than call db every time"""    

    query = database_connection(f"SELECT subject_id FROM ds_subjects_phenotypes WHERE { pub_check } \
    AND _data->>'data_version' = '{ data_version_id }' AND subject_type = '{ subject_type }'")

    dupe_list = [ id_tupe[ 0 ] for id_tupe in query ]

    return dupe_list

def check_not_duplicate( subject_id, dupecheck_list ):
    """takes id to check and the compiled dupe check and returns True if id is new, False if in dupe_list"""

    if subject_id in dupecheck_list:
        return False
    else:
        return True

def build_baseline_dupcheck_list( subject_type ):
    query = database_connection(f"SELECT subject_id FROM ds_subjects_phenotypes_baseline WHERE subject_type = '{ subject_type }'")
    baseline_dupe_list = [ id_tupe[ 0 ] for id_tupe in query ]

    return baseline_dupe_list

def check_not_dupe_baseline( subject_id, baseline_dupecheck_list ):
    """takes id to check and the compiled dupe check and returns True if id is new, False if in baseline_dupe_list"""

    if subject_id in baseline_dupecheck_list:
        return False
    else:
        return True

def get_data_version_id(release_version):
    """takes string release_version and returns id from data_version table"""
    query = database_connection(f"SELECT id FROM data_versions WHERE release_version = '{release_version}'")
    try:
        return query[0][0]
    except:
        print(f"No id found for release_version {release_version}. Check that the data_version has been added to the database")
        # then need to do something like return a signal that there's a problem
        return 'Release_version has not found in db'

def check_subject_exists(subject_type_view, subject_id, release_version):
    """takes id and release_version, makes sure that record exists for subject in target data version"""
    query = database_connection(f"SELECT COUNT(*) FROM {subject_type_view} where subject_id = '{subject_id}' AND release_version = '{release_version}'")

    if query[0][0] > 0:
        return True
    else:
        return False

def check_loadfile_variables_match_dictionary( data_dict, dictionary, subject_type, LOADFILE ):
    """Gets the appropriate dictionary, checks phenotype variables in are correct and complete"""
    keys_to_remove = ['update', 'correction', 'data_version', 'release_version', 'base_', 'subjid']
    dictionary_vars = [ var for var in dictionary[ "variable" ] ]
    modified_dictionary_varlist = []

    ## use next/iter to get first key in data_dict(ie. a subject id), then gets the keys for that subjects phenotype data dict
    data_dict_vars = [ var for var in data_dict[ next( iter( data_dict ) ) ].keys() ]

    for var in dictionary_vars:
        if var  != 'duplicate_subjid':
            if not any( keys in var for keys in keys_to_remove ):                
                modified_dictionary_varlist.append( var )
        else:
            modified_dictionary_varlist.append( var )

    if len( data_dict_vars ) == len( modified_dictionary_varlist ):
        for var in data_dict_vars:
            if var in modified_dictionary_varlist:
                continue
            else:
                return 0, f'{ var } is not in the { subject_type } dictionary.  Please check the correctness of your loadfile.'
        
        return 1, f'Variables in { LOADFILE } match those in dictionary for { subject_type }'

    else:
        ## if loadfile has more vars than dict
        if len( data_dict_vars ) > len( modified_dictionary_varlist ):
            var_diff = set( data_dict_vars ).difference( set( modified_dictionary_varlist ) )  
            return 0, f'{ LOADFILE } contains the following variable(s) not found in the dictionary: { var_diff }.  Please check loadfile for correctness.'
        else:
            var_diff = set( modified_dictionary_varlist ).difference( set( data_dict_vars ) )
            return 0, f'the following dictionary variables are missing from { LOADFILE }: { var_diff }.  Please check loadfile for correctness.'

def check_loadfile_correctness( LOADFILE, user_input_subject_type ):
    """takes loadfile and subject type, returns boolean indicating loadfile matches appropriate dict, along with a message"""
    """moved from initial_validation_check so can be used at beginning of any script"""
    data_dict = create_comparison_data_dict( LOADFILE, user_input_subject_type )
    dict_name = database_connection(f"SELECT dictionary_name FROM env_var_by_subject_type WHERE subject_type = '{ user_input_subject_type }'")[ 0 ][ 0 ]
    dictionary = get_dict_data( dict_name )

    variables_match_dictionary, msg = check_loadfile_variables_match_dictionary( data_dict, dictionary, user_input_subject_type, LOADFILE )
    
    return variables_match_dictionary, msg

# functions for handling user input
def get_filename():
    while True:
        try:
            filename_input = input(f"Enter loadfile name. ")
        except ValueError:
            continue
        if len(filename_input) < 4:
            print('Please enter a valid filename.')
            continue
        else:
            if '.csv' not in filename_input:
                print("Please make sure you've uploaded a .csv file.")
                continue
            else:
                filename = filename_input
                ## put check here to see that file exists
                try:
                    with open(f'./source_files/{filename}', mode='r', encoding='utf-8-sig') as csv_file:
                        pheno_file = csv.reader(csv_file)
                        break
                except:
                    print(f"{filename} not found, please confirm file in correct location and filename typed correctly.")
                    continue

    return filename
        
def get_subject_type():
    """takes nothing, returns subject_type for data being handled"""
    subject_types = [ type_tuple[ 0 ] for type_tuple in database_connection("SELECT DISTINCT subject_type FROM env_var_by_subject_type") ]

    while True:
        try:
            casefam_input = input(f"What type of data are you working with (select from list)? {subject_types}")
        except ValueError:
            continue
        if casefam_input in subject_types:
            print(f"Loading {casefam_input} data.")
            return casefam_input
        else:
            print("Please input a valid entry. ")
            continue

def get_publish_action():
    """takes nothing returns boolean value for publish status based on user input"""
    while True:
        try:
            pubstat_input = input(f"Do you want loaded data to be published? ")
        except ValueError:
            continue
        if pubstat_input in ['y', 'Y', 'yes', 'Yes', 'YES']:
            publish_status = True
            print("Loaded records will be given publish status.")
            return publish_status

        elif pubstat_input in ['n', 'N', 'no', 'No', 'NO']:
            print("Loaded records will no be published.")
            publish_status = False
            return publish_status
        else:
            print("Please input a valid entry. ")
            continue

def get_compare_query_type():
    """takes nothing as arg, and returns query type for comparison (ie. update vs. current or update vs. baseline"""
    while True:
        response_dict = {
            1: "update_to_latest",
            2: "update_to_baseline"
        }
        try:
            querytype_input = input(f"Select type of comparison (by key) to generate: {response_dict} ")
        except ValueError:
            continue
        if int(querytype_input) in response_dict.keys():
            query_type = response_dict[ int( querytype_input ) ]
            print(f"{query_type} comparison file will be generated. ")
            return query_type

        else:
            print("Please input a valid entry. Make sure you entered a numeric choice designation.")
            continue

def user_input_batch_loading():
    """takes no args, returns boolean for whether dropping subjects by batch file or manual input"""
    while True:
        try:
            isbatch_input = input(f"Are you dropping subjects via batchfile? ")
        except ValueError:
            continue
        if isbatch_input in ['y', 'Y', 'yes', 'Yes', 'YES']:
            isbatch_status = True
            print("You will be prompted to input loadfile name.")
            return isbatch_status

        elif isbatch_input in ['n', 'N', 'no', 'No', 'NO']:
            print("You will be prompted for subject_id and release_version of intended drop subject.")
            isbatch_status = False
            return isbatch_status
        else:
            print("Please input a valid entry. ")
            continue
        
def get_subject_to_drop(view_based_on_subject_type):
    """takes subject_type, returns dict with single entry, subject_id key with data_version table pkey as value"""
    single_dict = {}

    def get_release_version():
        """takes no args, returns release_version id after checking is there"""
        while True:
            try:
                release_name_input = input(f"Enter release_version for subjects to be dropped. ")
            except ValueError:
                continue
            if len(release_name_input) < 1:
                print('Please enter a release name.')
                continue
            else:
                data_version_id = get_data_version_id(release_name_input)
                if isinstance(data_version_id, int):
                    return data_version_id
                else:
                    print(f"{data_version_id}. Please check for correctness.")
                    continue
        
    def get_subject_id( data_version_id, view_to_check ):
        """returns subject_id inputed, after checking that it exists for given release_version"""
        while True:
            try:
                subject_id_input = input(f"Enter subject_id to be dropped. ")
            except ValueError:
                continue
            if len(subject_id_input) < 1:
                print('Please enter a subject_id.')
                continue
            else:
                release_version = database_connection(f"SELECT release_version FROM data_versions WHERE id = {data_version_id}")
                if check_subject_exists( view_to_check, subject_id_input, release_version[0][0] ):
                    return subject_id_input
                else:
                    print(f"No record for {subject_id_input} found for this release_version.  Check for correctness")
                    continue

    data_version_id = get_release_version()
    subject_id = get_subject_id( data_version_id, view_based_on_subject_type )

    if subject_id and data_version_id:
        single_dict[subject_id] = data_version_id
    
    return single_dict

def user_input_data_version():
    """takes nothing, returns subject_type for data being handled"""
    data_versions = [ version_tuple[ 0 ] for version_tuple in database_connection("SELECT DISTINCT release_version FROM data_versions") ]

    while True:
        try:
            version_input = input(f"Which release_version does your data belong to (select from list) Please only load data from one release at a time? {data_versions}")
        except ValueError:
            continue
        if version_input in data_versions:
            print(f"Loading {version_input} data.")
            return version_input
        else:
            print("Please input a valid entry. ")
            continue 

# log generators
def generate_errorlog():
    """creates error log and writes to 'log_files' directory"""
    if len(error_log) > 0:
        timestamp = calendar.timegm(time.gmtime())
        f = open(f'./log_files/{timestamp}-log.txt', 'w+')
        f.write(f'{str(len(error_log.items()))} flag(s) raised in runtime. See details below: \n\n')
        for key, value in error_log.items():
            f.write(f'Error: {value[1]} \n')

def generate_success_list():
    """creates a list of successfully created and inserted ADSP IDs"""

    if len(success_id_log) > 0:
        timestamp = calendar.timegm(time.gmtime())
        f = open(f'./log_files/success_lists/{timestamp}-generated_ids.txt', 'w+')
        for id in success_id_log:
            if success_id_log.index(id) >= len(success_id_log)-1:
                f.write(id)
            else:
                f.write(id + ', ')

        f.close()

def create_tsv( dataframe, subject_type ):
    """takes the compiled dataframe and subject-type (to formulate filename) and creates a TSV."""
    datestamp = datetime.date.today()
    corrected_subject_type = subject_type.replace( "/", "+" )

    dataframe.to_csv(f"./comparison_files/{ corrected_subject_type }-validation_errors-{ datestamp }.txt",sep="\t",index=True)

# fetching data
def get_dict_data( dict_name ):
    """takes dict name as arg, returns dataframe with dict info"""
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

    return data_df
    
def get_views_by_subject_type( subject_type ):
    """takes user-supplied subject_type, returns tuple of appropriate views from database table"""
    try:
        return database_connection(f"SELECT current_view_name, unpublished_update_view_name, baseline_view_name FROM env_var_by_subject_type WHERE subject_type = '{ subject_type }'")[ 0 ]
    except:
        print(f"No views found for datatype { subject_type }. Check database.  Program will exit.")
        sys.exit()

def build_release_dict():
    """takes no args, gets all the releases from the db with their string name and id, makes a dict with key
    as string name and id as value, so can replaces on the fly without 1 dbcall/row
    """
    query = database_connection("SELECT id, release_version FROM data_versions")
    release_dict = { version[ 1 ] : version[ 0 ] for version in query }

    return release_dict

def create_data_dict( LOADFILE, user_input_subject_type, publish_status, data_version, script_name ):
    """takes loadfile name as arg, returns dict of json data keyed by subject id of data to be entered in database"""
    """used in the loading and management scripts""" ## nb. 1/14/21 - could consolidate with create_comparison_data_dict ?
    scripts_requiring_pub_and_unpub_check = ['load_unpublished_updates.py']
    release_dict = build_release_dict()

    ## if running script in s_r_p_a_u_c list above, create the two checklists with hardcoded pub values.  Otherwise, where scripts can 
    ## run data that is either to be published or unpublished, make one checklist with pubstat depending on arg passed into function
    if script_name in scripts_requiring_pub_and_unpub_check:
        published_dupecheck_list = build_dupecheck_list( release_dict[ data_version ], 'PUBLISHED = TRUE', user_input_subject_type )
        unpublished_dupecheck_list = build_dupecheck_list( release_dict[ data_version ], 'PUBLISHED = FALSE', user_input_subject_type )

    ## 12/15 have to think about this more, right now 'else' runs if load_pheno or manage_updates, don't think in either of those cases want
    ## to worry about duplicate unpublished records.  
    else:
        dupecheck_list = build_dupecheck_list( release_dict[ data_version ], f'PUBLISHED = TRUE', user_input_subject_type )
    
    data_dict = {}

    with open(f'./source_files/{LOADFILE}', mode='r', encoding='utf-8-sig') as csv_file:
        """"get the relationship table names and indexes from the csv file headers"""
        pheno_file = csv.reader(csv_file)
        headers = next(pheno_file)
        
        for row in pheno_file:
            subject_id = row[ 0 ]
            if pheno_file.line_num > 1:
                blob = {}

                for index, value in enumerate( row ):

                    try:
                        blob[headers[ index ].lower()] = int( value )
                    except:
                        blob[headers[ index ].lower()] = value

                    blob[ "data_version" ] = release_dict[ data_version ]

                if 'subjid' in [ x.lower() for x in blob.keys() ]: ## handling for subjid/subject_id inconsistency
                    subject_id_varname = 'subjid'
                else:
                    subject_id_varname = 'subject_id'
                ## if need to check for published AND unpublished records for subject, use the two checklists created, 
                # otherwise, just the one with pubstat dependant on user input
                if script_name in scripts_requiring_pub_and_unpub_check:
                    if check_not_duplicate( blob[ subject_id_varname ], published_dupecheck_list ) and check_not_duplicate( blob[ subject_id_varname ], unpublished_dupecheck_list ):
                        data_dict[f'{ blob[subject_id_varname] }_{ data_version }'] = blob
                    else:
                        print( f'{ blob[ subject_id_varname ] } already has record in { data_version }')
                else:
                    if check_not_duplicate( blob[ subject_id_varname ], dupecheck_list ):
                        data_dict[f'{ blob[ subject_id_varname ] }_{ data_version }'] = blob
                    else:
                        print(f'{ blob[ subject_id_varname ] } already has record in { data_version }')

    return data_dict

def create_comparison_data_dict( LOADFILE, subject_type ):
    """takes loadfile, subject_type as args, returns dict of json data keyed by subject id of data to be valcheck"""
    """originally from initial_valcheck script"""
    release_dict = build_release_dict()
    data_dict = {}

    with open(f'./source_files/{LOADFILE}', mode='r', encoding='utf-8-sig') as csv_file:
        """"get the relationship table names and indexes from the csv file headers"""
        pheno_file = csv.reader(csv_file)
        headers = next(pheno_file)

        for row in pheno_file:
            if pheno_file.line_num > 1:
                blob = {}
                for index, value in enumerate(row):
                    try:
                        blob[headers[index].lower()] = int(value)
                    except:
                        blob[headers[index].lower()] = value
                    
                try: ## resolve subjid vs. subject_id errors between generated files, legacy data, database, etc. 
                    data_dict[ blob[ "subjid" ] ] = blob
                except:
                    data_dict[ blob[ "subject_id" ] ] = blob

    for key, record in data_dict.items():
        """remove subject id from blob for each record in dict"""
        try: #deal with subjid vs subject_id
            record.pop( 'subjid' )
        except KeyError:
            record.pop( 'subject_id' )

    return data_dict

# for dev/debugging
def write_json_to_file( json_data ):
    """for checking data and ect, takes json and writes as json file"""
    with open('data.json', 'w') as outfile:
        json.dump( json_data, outfile )

def get_test_json_from_file( filename ):
    """takes json file and reads to dict for testing functions w/o db read"""
    with open(f'./source_files/{ filename }') as json_file:
        data = json.load(json_file)

    return data