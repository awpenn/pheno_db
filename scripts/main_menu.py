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
import pheno_utils

def main():
    function_dict = {
        "1": initial_validation_checks.main,
        "2": load_unpublished_updates.main,
        "3": compare.main,
        "4": update_validation_checks.main,
        "5": manage_updates.main,
        "6": drop_subjects.main,
        "7": load_dicts.main,
        "8": get_dict.main,
    }

    funcname_dict = {
        "1": "Run intial validation checks\n",
        "2": "Load unpublished phenotypes\n",
        "3": "Generate comparison file\n",
        "4": "Validate phenotype update data\n",
        "5": "Manage updates\n",
        "6": "Drop subjects\n",
        "7": "Load dictionaries\n",
        "8": "Get dictionaries\n",
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