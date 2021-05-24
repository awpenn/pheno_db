import sys
sys.path.append('/home/pheno_db/.venv/lib/python3.6/site-packages/')

import psycopg2
import csv
from dotenv import load_dotenv
import os
import json
import datetime

import pandas as pd

error_log = {}
success_ids_log = {
    "release": '',
    "ids": []
}

load_dotenv()
DBIP = os.getenv('DBIP')
DBPASS = os.getenv('DBPASS')
DBPORT = os.getenv('DBPORT')
DB = os.getenv('DB')
DBUSER = os.getenv('DBUSER')

## set to true if 'DEBUG' passed as cmdline arg, prevents database writing and etc.
DEBUG = False
## these are tests that can be toggled in validation 
checks_to_toggle = {
    "1": 'braak_inc_prev',
}

## for confirmation queries
valid_confirm_inputs = [ 'y', 'n' ]

def database_connection( query, params ):
    """takes a string SQL statement as input, and depending on the type of statement either performs an insert or returns data from the database"""
    global DB
    try:
        connection = psycopg2.connect(user = DBUSER, password = DBPASS, host = DBIP, port = DBPORT, database = DB)
        cursor = connection.cursor( )
        cursor.execute( query, params )

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
        raise ValueError
    

    finally:
        if(connection):
            cursor.close()
            connection.close()

def change_data_version_published_status( data_version_published_status, data_version ):
    """takes user-input publish status for data version and data_version id, returns nothing"""

    database_connection( f"UPDATE data_versions SET Published = { data_version_published_status } WHERE id = %s", ( data_version, ) )

#check-functions for data correctness
def build_dupecheck_list( data_version_id, pub_check, subject_type ):
    """takes version, publication status and subject type as args,
    returns list of subjects for a particular release version to check for duplicates rather than call db every time"""    

    query = database_connection(f"SELECT subject_id FROM ds_subjects_phenotypes WHERE { pub_check } \
    AND _data->>'data_version' = '%s' AND subject_type = '{ subject_type }'", ( data_version_id, ) )

    dupe_list = [ id_tupe[ 0 ] for id_tupe in query ]

    return dupe_list

def check_not_duplicate( subject_id, dupecheck_list ):
    """takes id to check and the compiled dupe check and returns True if id is new, False if in dupe_list"""

    if subject_id in dupecheck_list:
        return False
    else:
        return True

def build_baseline_dupcheck_list( subject_type ):

    query = database_connection( f"SELECT subject_id FROM ds_subjects_phenotypes_baseline WHERE subject_type = '{ subject_type }'", ( ) )
    baseline_dupe_list = [ id_tupe[ 0 ] for id_tupe in query ]

    return baseline_dupe_list

def check_not_dupe_baseline( subject_id, baseline_dupecheck_list ):
    """takes id to check and the compiled dupe check and returns True if id is new, False if in baseline_dupe_list"""

    if subject_id in baseline_dupecheck_list:
        return False
    else:
        return True

def get_data_version_id( release_version ):
    """takes string release_version and returns id from data_version table"""

    query = database_connection(f"SELECT id FROM data_versions WHERE release_version = '{release_version}'", ( ) )
    try:
        return query[0][0]
    except:
        print(f"No id found for release_version {release_version}. Check that the data_version has been added to the database")
        # then need to do something like return a signal that there's a problem
        return 'Release_version has not found in db'

def check_subject_exists(subject_type_view, subject_id, release_version):
    """takes id and release_version, makes sure that record exists for subject in target data version"""

    query = database_connection(f"SELECT COUNT(*) FROM { subject_type_view } where subject_id = %s AND release_version = %s", ( subject_id, release_version ) )

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
                return 0, f'"{ var }" is not in the { subject_type } dictionary.  Please check the correctness of your loadfile.'
        
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
    dict_name = database_connection( f"SELECT dictionary_name FROM env_var_by_subject_type WHERE subject_type = '{ user_input_subject_type }'", ( ) )[ 0 ][ 0 ]
    dictionary = get_dict_data( dict_name )

    variables_match_dictionary, msg = check_loadfile_variables_match_dictionary( data_dict, dictionary, user_input_subject_type, LOADFILE )
    
    return variables_match_dictionary, msg

# functions for handling user input
def select_database_type( ):
    """takes no arguments, returns value for DB attribute in database connection, so user can select sandbox or production"""
    while True:
        try:
            db_input = input(f"Do you want to load data to the sandbox or production database? (Select number). 1: sandbox, 2: production ")
        except ValueError:
            continue
        
        if not db_input:
            continue

        elif not db_input.isdigit():
            continue

        elif db_input not in ['1', '2']:
            print('Please select an option from the list.')
            continue

        else:
            if int( db_input ) == 1:
                return 'SANDBOX'
            else:
                confirm_input = input( f"You have selected the production database. Please confirm.(y/n) " )
                valid_confirm_input = [ 'y', 'n' ]
                if confirm_input.lower() not in valid_confirm_input:
                    continue
                else:
                    if confirm_input.lower() == 'y':
                        return 'production'        
                    else:
                        continue

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

    subject_types = [ type_tuple[ 0 ] for type_tuple in database_connection( "SELECT DISTINCT subject_type FROM env_var_by_subject_type", ( ) ) ]

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
            print("Loaded records will not be published.")
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
        
def get_subject_to_drop( view_based_on_subject_type ):
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
                release_version = database_connection( f"SELECT release_version FROM data_versions WHERE id = { data_version_id }", ( ) )
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
    """takes no arg, returns dataversion for data being handled"""
    ## only returns versions not published, so once published, data cant be overwritten accidently 
    data_versions = [ version_tuple[ 0 ] for version_tuple in database_connection( "SELECT DISTINCT release_version FROM data_versions WHERE published = FALSE", ( ) ) ]

    while True:
        try:
            version_input = input(f"Which release_version does your data belong to (select from list)? Please only load data from one release at a time. { data_versions }")
        except ValueError:
            continue
        if version_input in data_versions:
            print(f"Loading {version_input} data.")
            return version_input
        else:
            print("Please input a valid entry. ")
            continue 

def user_input_publish_dataset( data_version_string, write_counter ):

    qstring = f"SELECT COUNT(*) FROM ds_subjects_phenotypes \
                JOIN data_versions \
                    ON CAST(ds_subjects_phenotypes._data::json->>'data_version' AS INT) = data_versions.id \
                        WHERE data_versions.release_version = '{ data_version_string }'"
    subjects_by_version_in_db = database_connection( qstring, ( ) )

    while True:
        try:
            publish_input = input(f"{ write_counter } Phenotypes loaded for { data_version_string }. { subjects_by_version_in_db[ 0 ][ 0 ] } total phenotype records for { data_version_string }  Are you ready to publish dataset?(y/n) ")
        except ValueError:
            continue
        if publish_input not in [ 'y', 'n' ]:
            print('Invalid input.')
            continue
        else:
            if publish_input == 'y':
                return True
            else:
                return False

def user_input_toggle_checks( subject_type, checktype ):
    """"takes subject_type and checktype, asks user what checks they want to toggle off, returns list compiled from `checks_to_toggle` dict"""
    selected_checks_to_toggle = []

    def add_check_to_toggle_list( check_name ):
        selected_checks_to_toggle.append( checks_to_toggle[ select_input ] )

    def initial_confirm( ):
        while True:
            try:
                confirm_input = input( 'Do you want to toggle off any validation checks?(y/n) ' )
            except ValueError:
                continue
            if not confirm_input:
                print( 'Please enter y/n value' )
                continue
            elif confirm_input.isdigit( ):
                print( 'Please enter y/n value' )
                continue
            elif confirm_input.lower( ) not in valid_confirm_inputs:
                print( 'Please enter y/n value' )
                continue
            else:
                if confirm_input.lower( ) == 'y':
                    return True
                else:
                    return False

    toggle_off = initial_confirm( )

    if toggle_off:
        while True:
            try:
                select_input = input( f"What checks would you like to toggle (enter numerical from list)? { ', '.join( [ f'{ key }: { value }' for key, value in checks_to_toggle.items( ) ] ) } " )
            except ValueError:
                continue
            if not select_input:
                print( 'Please enter value from list' )
                continue
            elif select_input not in checks_to_toggle.keys( ):
                print( 'Please enter value from list' )
                continue
            else:
                if checks_to_toggle[ select_input ] not in selected_checks_to_toggle:
                    add_check_to_toggle_list( checks_to_toggle[ select_input ] )
            
                while True:
                    try: 
                        confirm_input = input( f"Tests toggled off: { ', '.join( [ check for check in selected_checks_to_toggle ] ) }. Select more?(y/n) " )
                    except ValueError:
                        continue
                    if not confirm_input:
                        print( 'Please enter y/n value' )
                        continue
                    elif confirm_input.isdigit( ):
                        print( 'Please enter y/n value' )
                        continue
                    elif confirm_input.lower( ) not in valid_confirm_inputs:
                        print( 'Please enter y/n value' )
                        continue
                    else:
                        if confirm_input.lower( ) == 'n':
                            return selected_checks_to_toggle
                            break
                        else:
                            break
                continue
    else: ##user decided to not choose any checks to toggle off
        return None

# log generators
def generate_errorlog( ):
    """
    creates error log and writes to 'log_files' directory

    error log is dict, keyed by entry number, each entry is list with string error in first position and (non-required?) data object in second
    """
    if len( error_log ) > 0:
        date = datetime.date.today( )
        time = datetime.datetime.now( ).strftime("%H-%M-%S")

        f = open(f'./log_files/error_logs/{ date }-{ time }-log.txt', 'w+')
        f.write( f'{ str( len( error_log.items( ) ) ) } flag(s) raised in runtime. See details below: \n\n')
        for key, value in error_log.items( ):
            f.write( f"{ value[ 0 ] }\n" )
                
            if len( value ) > 1:
                f.write( "Relevant error data\n" )
                for errkey, errvalue in value [ 1 ].items( ):
                    f.write( f"{ errkey }: { errvalue }\n" )

            f.write( "\n\n" )

def generate_success_list( ):
    """no args, creates a list of successfully created and inserted ADSP IDs"""

    if len( success_ids_log[ 'ids' ] ) > 0:
        date = datetime.date.today( )
        time = datetime.datetime.now( ).strftime("%H-%M-%S")
        f = open(f'./log_files/success_lists/{ date }-{ time }-phenotype_records_added.txt', 'w+')
        f.write( f"Subject records added to database for { success_ids_log[ 'release' ] }\n\n" )

        for id in success_ids_log[ 'ids' ]:
            f.write( f'{ id }\n')

        f.close( )

def create_tsv( dataframe, subject_type, validation_type, requires_index = False ):
    """takes the compiled dataframe and subject-type (to formulate filename), validation_type (if initial or update checks), and creates a TSV."""
    datestamp = datetime.date.today()
    corrected_subject_type = subject_type.replace( "/", "+" )

    if requires_index:
        dataframe.to_csv(f"./validation_error_files/{ corrected_subject_type }-{ validation_type }_errors-{ datestamp }.txt", sep="\t" )
    else:
        dataframe.to_csv(f"./validation_error_files/{ corrected_subject_type }-{ validation_type }_errors-{ datestamp }.txt", sep="\t", index=False )

def generate_summary_report( data_dict, user_input_subject_type, loadtype ):
    """called at end of write to database funct, args = the write_to_db dict, loadtype (publish or update) and the subect_type..."""

    update_columns_by_subject_type = {
        "case/control": 'subject_id, update_baseline, update_latest, update_adstatus, correction', 
        "family": 'subject_id, update_baseline, update_latest, update_adstatus, correction',
        "PSP/CDB": 'subject_id, update_baseline, update_latest, update_diagnosis, correction', 
        "ADNI": 'subject_id, update_baseline, update_latest, update_diagnosis, correction', 
    }
    
    ## taking data dict and keying by subjid rather than subjid+version
    update_data_dict = { record[ 'subject_id' ]: record for key, record in data_dict.items( ) }

    if loadtype == 'unpublished_update':
        current_view = database_connection( f"SELECT current_view_name FROM env_var_by_subject_type WHERE subject_type = '{ user_input_subject_type }'", ( ) )[ 0 ][ 0 ]
        ## get update columns from subject_type current view and key by subject_id
        retrieved_data = { record[ 0 ]: record for record in database_connection( f"SELECT { update_columns_by_subject_type[ user_input_subject_type ] }  FROM { current_view }", ( ) ) }

    elif loadtype == 'new_published_release':
        ##figure out what this published view is, get the previous one, use that in where clause below to get comparison data
        retrieval_view = database_connection( f"SELECT all_view_name FROM env_var_by_subject_type WHERE subject_type = '{ user_input_subject_type }'", ( ) )[ 0 ][ 0 ]

        ## get data version id from first subject in dict of data from current load
        loaded_data_data_version = update_data_dict[ list( update_data_dict.keys( ) )[ 0 ] ][ 'data_version' ]

        ordered_list_data_versions = [ data_version[ 0 ] for data_version in database_connection( f"SELECT DISTINCT (data_version) FROM { retrieval_view } ORDER BY data_version DESC", ( ) ) ]
        ## ordered_list_data_versions = [ data_version[ 0 ] for data_version in database_connection( f"SELECT DISTINCT (data_version) FROM { retrieval_view } WHERE version_published = True ORDER BY data_version DESC", ( ) ) ]
        
        ## take current load's version id, get index of that from DESC ordered list of data versions above, get the version that follows it in list

        if len( ordered_list_data_versions ) > 1:
            previous_latest_version = ordered_list_data_versions[ ordered_list_data_versions.index( loaded_data_data_version ) + 1 ]
        else:
            previous_latest_version = ordered_list_data_versions[ 0 ]

        retrieved_data = { record[ 0 ]: record for record in database_connection( f"SELECT { update_columns_by_subject_type[ user_input_subject_type ] }  FROM { retrieval_view } WHERE data_version = '{ previous_latest_version }'", ( ) ) }

    ## build dict of latest published data for subject_type
    retrieved_data_dict = { }
    for key, record in retrieved_data.items( ):
        blob = {}
        ## split the columns string from `update_columns_by_subject_type` into list and enumerate
        for index, header in enumerate( update_columns_by_subject_type[ user_input_subject_type ].split( ', ' ) ):
            ## headers index will match index for that data value in the `retrieved_data` (`record` in first for loop)
            blob[ header ] = record[ index ]
        
        ## add all the labeled datapoints to the last_published dict, keyed by subject_id
        retrieved_data_dict[ key ] = blob
    
    ## build dict to hold report info
    report_dict = {
        "new_subjects": {
            "count": 0,
            "subject_ids": [],
        },
        "updated_subjects": {
            "count": 0,
            "subject_ids": [],
        },
        "ad/diagnosis_update_subjects": {
            "count": 0,
            "subject_ids": [],
        },
        "correction_subjects": {
            "count": 0,
            "subject_ids": []
        },
    }

    ## count subjects in update not in latest published
    for key in update_data_dict.keys( ):
        if key not in retrieved_data_dict.keys( ):
            report_dict[ 'new_subjects' ][ 'count' ] += 1
            report_dict[ 'new_subjects' ][ 'subject_ids' ].append( key )
        
        ## count subjects newly updated
        if update_data_dict[ key ][ 'update_latest' ] == 1:
            report_dict[ 'updated_subjects' ][ 'count' ] += 1
            report_dict[ 'updated_subjects' ][ 'subject_ids' ].append( key )
    
        ## count corrections new to update
        if update_data_dict[ key ][ 'correction' ] == 1: ## ...but the update has a correction
            report_dict[ 'correction_subjects' ][ 'count' ] += 1
            report_dict[ 'correction_subjects' ][ 'subject_ids' ].append( key )

        ## count ad/diagnosis update, depending on subject_type
        if 'update_adstatus' in update_columns_by_subject_type[ user_input_subject_type ]: ##for cc/fam
            if update_data_dict[ key ][ 'update_adstatus' ] == 1:
                report_dict[ 'ad/diagnosis_update_subjects' ][ 'count' ] += 1
                report_dict[ 'ad/diagnosis_update_subjects' ][ 'subject_ids' ].append( key )
        elif 'update_diagnosis' in update_columns_by_subject_type[ user_input_subject_type ]: ##for ADNI/psp
            if update_data_dict[ key ][ 'update_diagnosis' ] == 1:
                report_dict[ 'ad/diagnosis_update_subjects' ][ 'count' ] += 1
                report_dict[ 'ad/diagnosis_update_subjects' ][ 'subject_ids' ].append( key )

    ## build report file
    date = datetime.date.today( )
    time = datetime.datetime.now( ).strftime("%H-%M-%S")

    if loadtype == 'unpublished_update':
        f = open(f'./log_files/{ date }-{ time }-update-report.txt', 'w+')

        f.write( f"SUMMARY REPORT FOR UNPUBLISHED UPDATE\n\n" )

    elif loadtype == 'new_published_release':
        f = open(f'./log_files/{ date }-{ time }-new-release-report.txt', 'w+')

        f.write( f"SUMMARY REPORT FOR NEW RELEASE\n\n" )

    ##new subjects
    f.write( f"Subject not in previous release: { report_dict[ 'new_subjects' ][ 'count' ] }\n" )
    for subject in report_dict[ 'new_subjects' ][ 'subject_ids' ]:
        f.write( f"{ subject }\n" )
    f.write("\n\n")

    ##updates
    f.write( f"Subjects with updated phenotypes: { report_dict[ 'updated_subjects' ][ 'count' ] }\n" )
    for subject in report_dict[ 'updated_subjects' ][ 'subject_ids' ]:
        f.write( f"{ subject }\n" )
    f.write("\n\n")

    ##updates to diagnosis
    f.write( f"Subjects with updated ad/diagnosis: { report_dict[ 'ad/diagnosis_update_subjects' ][ 'count' ] }\n" )
    for subject in report_dict[ 'ad/diagnosis_update_subjects' ][ 'subject_ids' ]:
        f.write( f"{ subject }\n" )
    f.write("\n\n")

    ##corrections
    f.write( f"Subjects with new corrections: { report_dict[ 'correction_subjects' ][ 'count' ] }\n" )
    for subject in report_dict[ 'correction_subjects' ][ 'subject_ids' ]:
        f.write( f"{ subject }\n" )
    f.write("\n\n")

# data handling
def get_unpublished_subjects_by_release( subject_type ):
    """takes subject_type str arg, returns dict keyed by release name, with list of subjects with unpublished ids in database"""

    ## initialize dict to hold data
    unpublished_subjects_by_release_dict = {}

    ## get dict of release names, keyed by their str'd tablekey
    releases = { str( vers[ 0 ] ): vers[ 1 ] for vers in database_connection( "SELECT id, release_version FROM data_versions", ( ) ) }

    ## get unpublished subjects in ds_subjects_phenotypes table, keyed by tablekey
    subjects = { subject[ 0 ]: {
        "subject_id": subject[ 1 ],
        "data_version": subject[ 2 ],
    } for subject in database_connection( f"SELECT id, subject_id, _data->>'data_version' FROM ds_subjects_phenotypes WHERE published = FALSE AND subject_type = '{ subject_type }'", ( ) ) }

    ## sort the subjects into a nested dict keyed by release name, with str value release tablekey and a list of unpublished subjects
    for key, release in releases.items( ):
        unpublished_subjects_by_release_dict[ release ] = { "release_tablekey": key, "unpublished_subjects": [ ] } ## create nested dict with release tablekey and empty list to hold subject_ids

        for skey, subject in subjects.items( ):
            if subject[ 'data_version' ] == key: ## if the key ( tablekey of versions table ) equals the current subject's data_version value
                unpublished_subjects_by_release_dict[ release ][ "unpublished_subjects" ].append( subject[ 'subject_id' ] )

    return unpublished_subjects_by_release_dict

def get_dict_data( dict_name ):
    """takes dict name as arg, returns dataframe with dict info"""
    dict_data = [ return_tuple[ 0 ] for return_tuple in database_connection( f"SELECT _dict_data FROM data_dictionaries WHERE dictionary_name = %s", ( dict_name, ) ) ][ 0 ]
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
        return database_connection( f"SELECT current_view_name, unpublished_update_view_name, baseline_view_name FROM env_var_by_subject_type WHERE subject_type = '{ subject_type }'", ( ) )[ 0 ]
    except:
        print(f"No views found for datatype { subject_type }. Check database.  Program will exit.")
        sys.exit()

def build_release_dict():
    """takes no args, gets all the releases from the db with their string name and id, makes a dict with key
    as string name and id as value, so can replaces on the fly without 1 dbcall/row
    """

    query = database_connection("SELECT id, release_version FROM data_versions", ( ) )
    release_dict = { version[ 1 ] : version[ 0 ] for version in query }

    return release_dict

def create_data_dict( LOADFILE, user_input_subject_type, data_version, script_name ):
    """takes loadfile name as arg, returns dict of json data keyed by subject id of data to be entered in database"""
    """used in the loading and management scripts""" ## nb. 1/14/21 - could consolidate with create_comparison_data_dict ?
    scripts_requiring_pub_and_unpub_check = [ 'load_unpublished_updates.py' ]
    release_dict = build_release_dict()

    ## if running script in s_r_p_a_u_c list above, create the two checklists with hardcoded pub values.  Otherwise, where scripts can 
    ## run data that is either to be published or unpublished, make one checklist with pubstat depending on arg passed into function
    if script_name in scripts_requiring_pub_and_unpub_check:
        published_dupecheck_list = build_dupecheck_list( release_dict[ data_version ], 'PUBLISHED = TRUE', user_input_subject_type )
        unpublished_dupecheck_list = build_dupecheck_list( release_dict[ data_version ], 'PUBLISHED = FALSE', user_input_subject_type )

    ## 12/15/20 have to think about this more, right now 'else' runs if load_pheno or manage_updates, don't think in either of those cases want
    ## to worry about duplicate unpublished records.  
    else:
        dupecheck_list = build_dupecheck_list( release_dict[ data_version ], 'PUBLISHED = TRUE', user_input_subject_type )
    
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
                        blob[headers[ index ].lower()] = value.strip( )##remove whitespace from string values

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

    with open( f'./source_files/{ LOADFILE }', mode='r', encoding='utf-8-sig' ) as csv_file:
        """"get the relationship table names and indexes from the csv file headers"""
        pheno_file = csv.reader( csv_file )
        headers = next( pheno_file )

        for row in pheno_file:
            if pheno_file.line_num > 1:
                blob = {}
                for index, value in enumerate( row ):
                    try:
                        blob[ headers[ index ].lower( ) ] = int( value )
                    except:
                        blob[ headers[ index ].lower( ) ] = value.strip( )
                    
                try: ## resolve subjid vs. subject_id errors between generated files, legacy data, database, etc. 
                    data_dict[ blob[ "subjid" ] ] = blob
                except:
                    data_dict[ blob[ "subject_id" ] ] = blob

    for key, record in data_dict.items( ):
        """remove subject id from blob for each record in dict"""
        try: #deal with subjid vs subject_id
            record.pop( 'subjid' )
        except KeyError:
            record.pop( 'subject_id' )

    return data_dict
#
def create_baseline_json( data ):
    """takes dict entry for subject being added to database and creates the copy of data for baseline table, returning json string"""
    baseline_data = {}
    for key, value in data.items():
        if "update" not in key and "correction" not in key:
            baseline_data[key] = value
 
    return json.dumps( baseline_data )

def save_baseline( baseline_dupecheck_list, subject_id, data, user_input_subject_type ):
    """takes data dict and writes to database"""

    _baseline_data = create_baseline_json( data )

    if check_not_dupe_baseline( subject_id , baseline_dupecheck_list ):
        database_connection( f"INSERT INTO ds_subjects_phenotypes_baseline(subject_id, _baseline_data, subject_type) VALUES(%s, %s, %s)", ( subject_id, _baseline_data, user_input_subject_type ) ) 
    else:
        print(f'There is already a {user_input_subject_type} baseline record for {subject_id}.')

def get_phenotype_and_consent_level_data( database_type, subject_type ):
    """takes string for database type and string representing subject type, returns 2 dicts subject dict and dict of subjects keyed by consent"""
    global DB
    DB = os.getenv('DB')

    ## get subject type and the [type]_current view
    current_release_view = database_connection( f"SELECT current_view_name FROM env_var_by_subject_type WHERE subject_type = '{ subject_type }'" , ( ) )[ 0 ][ 0 ]
    
    ## get phenotype data based on selected subject_type
    pheno = database_connection( f"SELECT * FROM { current_release_view }" , ( ) )
    pheaders = [ record[ 0 ] for record in database_connection( f"SELECT column_name FROM information_schema.columns WHERE table_name = '{ current_release_view }'", ( ) ) ]
    
    ## if there where phenotype records returned, create a dict of phenotype data keyed by adspid
    if pheno:
        pheno_dict = { }
        ## take pheno data and add the headers, make dict
        for record in pheno:
            blob = {}
            for index, phenotype in enumerate( record ):
                blob.update( { pheaders[ index ]: phenotype } )

            pheno_dict[ record[ 0 ] ] = blob
    else:
        print( f'No published { subject_type } records in phenotype database.' )
        sys.exit( )

    DB = f'{ database_type }_adspid_database'
    
    ## create dict of site_id, adsp_id, and cohort_code from the builder_lookup view, keyed by adspid
    adspid_database_dict = { record[ 1 ]: { "site_indiv_id": record[ 0 ], "cohort": record[ 2 ] } for record in database_connection("SELECT site_indiv_id, adsp_id, cohort_identifier_code FROM builder_lookup" , ( ) ) }

    ## adds site_indiv_id from adspid database to pheno_dict (built from phenodb)
    for key, value in list( pheno_dict.items( ) ):## have to make a listcopy of dict so can delete phenotype records from dict that dont have data in adspid database
        if key in adspid_database_dict.keys( ):
            value.update( {"site_indiv_id": adspid_database_dict[ key ][ 'site_indiv_id' ] } )
            value.update( {"cohort": adspid_database_dict[ key ][ 'cohort' ] } )
        else:
            print( f"No adsp_id database record found for { key }" )
            del pheno_dict[ key ]

    DB = f'{ database_type }_consent_database'

    ## create dict of site_id, resolved consent code, and cohort code from subjects_cohorts_consents view, keyed by uid generated by concat of cohort_code and site_id
    consent_database_dict = { f'{ record[ 2 ] }-{ record[ 0 ] }': { "consent": record[ 1 ], "cohort": record[ 2 ] } for record in database_connection( "SELECT subject_id, resolved_consent_name, subjects_cohort_name FROM subjects_cohorts_consents", ( ) ) }
    
    ## add consent to pheno_dict
    for key, value in list( pheno_dict.items( ) ):##make list so can remove from dict that arent found in cdb
        uid = f"{ value[ 'cohort' ] }-{ value[ 'site_indiv_id' ] }" ##make uid for each entry in subjects_dict to match key of cdb dict
        if uid in consent_database_dict.keys( ):
            value.update( { "resolved_consent": consent_database_dict[ uid ][ 'consent' ] } ) 
        else:
            print( f'No consent database record found for { key }' )
            del pheno_dict[ key ]
    
    ## make set of consents found in phenotype data dict
    consents_set = set( )
    for key, value in pheno_dict.items( ):
        consents_set.add( value[ "resolved_consent" ] )
    
    by_consents_dict = {}

    ## create nested dict (to go in by_consents_dict), keyed by clevel, of every subject found in dict with that consent level
    for consent in consents_set:
        blob = {}
        for key, subject in pheno_dict.items( ):
            if subject[ 'resolved_consent' ] == consent:
                blob[ key ] = subject

        by_consents_dict[ consent ] = blob

    ## return phenotype dict ( keyed by adspid ) and consents dict ( keyed by consent level )
    return pheno_dict, by_consents_dict

#
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

def check_DEBUG( ):
    global DEBUG
    if len( sys.argv ) > 1:
        if sys.argv[ 1 ] == 'DEBUG':
            DEBUG = True