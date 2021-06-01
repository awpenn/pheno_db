import sys
sys.path.append('/home/pheno_db/.venv/lib/python3.6/site-packages/')
from dotenv import load_dotenv
import pheno_utils
import os 

import compare
import get_dict
import load_dicts
import drop_subjects
import legacy_loader
import manage_updates
import load_phenotypes
import initial_validation_checks
import update_validation_checks
import load_unpublished_updates
import publish_data_version
import pheno_utils
import get_phenotypes_and_consents
import get_phenotypes_split_by_consents
import create_new_release

def main():
    ## resets the DB to phenotype database before commands are run, in case previously run command involed connections to other dbs
    load_dotenv( )
    pheno_utils.DB = os.getenv( 'DB' )

    function_dict = {
        "1": initial_validation_checks.main,
        "2": create_new_release.main,
        "3": drop_subjects.main,
        "4": load_unpublished_updates.main,
        "5": compare.main,
        "6": update_validation_checks.main,
        "7": manage_updates.main,
        "8": publish_data_version.main,
        "9": get_phenotypes_and_consents.main,
        "10": get_phenotypes_split_by_consents.main,
        "11": load_dicts.main,
        "12": get_dict.main,
    }

    funcname_dict = {
        "1": "Run initial validation checks\n",
        "2": "Create new release\n",
        "3": "Drop subjects from update\n",
        "4": "Load new subjects/updates to existing subjects\n",
        "5": "Generate comparison file\n",
        "6": "Run update validation checks\n",
        "7": "Makes changes to updates\n",
        "8": "Publish new release\n",
        "9": "Get phenotypes and consent levels\n",
        "10": "Get phenotypes and consent levels, split by consent\n",
    }
    
    function_input = input( f'\nWhat action would you like to take?(Select numerical value) \n{ "".join( [ f"{ key }: { value }" for key, value in funcname_dict.items() ] ) } ' )

    if function_input in function_dict.keys():
        call_function( function_dict, function_input )
    else:
        print('Invalid entry. ')
        main()
        
def call_function( function_dict, fname, **kwargs ):
    if kwargs:
        function_dict[ fname ](  **kwargs )
    else:
        function_dict[ fname ]()

    while True:
        valid_confirm_answers = ['y', 'n']
        try:
            more_work_input = input( '\nWould you like to run another command? (y/n) ' )
        except ValueError:
            continue
        if more_work_input.lower() not in valid_confirm_answers:
            print('Please enter a valid input (y/n).')
            continue
        else:
            if more_work_input.lower() ==  'y':
                ## reset errorlog and success ids object in pheno_utils
                pheno_utils.error_log = {}
                pheno_utils.success_ids_log = {
                                "release": '',
                                "ids": []
                            }
                main( )
                break

            else:
                print( 'Ending program.' )
                break
            
if __name__ == '__main__':
    main()