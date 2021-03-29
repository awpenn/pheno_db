import compare
import get_dict
import load_dicts
import drop_subjects
import legacy_loader
import manage_updates
import load_phenotypes
import load_unpublished_updates

def main():
    function_dict = {
        "1": compare.main,
        "2": get_dict.main,
        "3": load_dicts.main,
        "4": drop_subjects.main,
        "5": legacy_loader.main,
        "6": manage_updates.main,
        "7": load_phenotypes.main,
        "8": load_unpublished_updates.main,
    }

    funcname_dict = {
        "1": "compare",
        "2": "get_dict",
        "3": "load_dicts",
        "4": "drop_subjects",
        "5": "legacy_loader",
        "6": "manage_updates",
        "7": "load_phenotypes",
        "8": "load_unpublished_updates",
    }
    
    function_input = input( f'\nWhat action would you like to take?(Select numerical value) { ", ".join( [ f"{ key }: { value }" for key, value in funcname_dict.items() ] ) } ' )

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
                main()
                break
            else:
                print( 'Ending program.' )
                break
if __name__ == '__main__':
    main()