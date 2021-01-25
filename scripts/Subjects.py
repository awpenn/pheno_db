"""
classes for different datatypes in phenotype database, contain checks required for update_validation.  
takes the individual subject's phenotype data ( "subject_data" ) as argument, also the whole comparison data object ( "all_data" ) for doing
things like checking family data
"""

import pandas as pd
import json

from pheno_utils import *

#utils
def handle_age_values( pheno_value ):
    """takes the age-related pheno-value and handles for string values, ie. where an age like '90+' """
    if isinstance( pheno_value, str ):
        if pheno_value == 'NA':
            processed_pheno_value = pheno_value
        else:
            processed_pheno_value = int( pheno_value.replace("+", "") )
    else:
        processed_pheno_value = pheno_value

    return processed_pheno_value

### Parent Classes with variables/functions shared by child classes
class Non_PSP_Subject:
    def __init__( self, subject_id, subject_data, all_data, checktype ):

        self.subject_id = subject_id
        self.age_baseline = handle_age_values( subject_data[ "age_baseline" ] )
        self.apoe = subject_data[ "apoe" ]
        self.autopsy = subject_data[ "autopsy" ]
        self.braak = subject_data[ "braak" ]
        self.ethnicity = subject_data[ "ethnicity" ]
        self.race = subject_data[ "race" ]
        self.sex = subject_data[ "sex" ]

        if checktype == 'initial-validation':
            self.comments = subject_data[ "comments" ]

        if checktype == "update-validation":
            self.subject_id = subject_data[ "subject_id" ]
            self.previous_age_baseline = handle_age_values( subject_data[ "prev_age_baseline" ] )
            self.previous_apoe = subject_data[ "prev_apoe" ]
            self.previous_autopsy = subject_data[ "prev_autopsy" ]
            self.previous_braak = subject_data[ "prev_braak" ]
            self.previous_ethnicity = subject_data[ "prev_ethnicity" ]
            self.previous_race = subject_data[ "prev_race" ]
            self.previous_sex = subject_data[ "prev_sex" ]
            self.previous_comments = subject_data["prev_comments"]

        self.all_data = all_data

        self.data_errors = {}

    ## checks run as part of initial and update validation ( ie. no comparison data required )
    def check_for_blank_values( self ):
        """enumerates over object properties, checks that all (excluding comments) have a given value"""  
        variables_to_skip = [ 'dictionary', 'all_data', 'data_errors' ] 
        blank_variable_list = []
        for variable, value in vars( self ).items():
            if variable not in variables_to_skip:
                if value == '' and variable.lower() != 'comments':
                    blank_variable_list.append( variable )
        
        if blank_variable_list:
            blank_var_string = ', '.join( blank_variable_list )
            self.data_errors [ 'blank_value_check' ] = f"One or more variables found with blank values: { blank_var_string }"

    def check_data_values_against_dictionary( self ):
        variables_to_skip = [ 'subject_id', 'dictionary', 'all_data', 'data_errors', 'subject_type', 'comments' ]
        data_value_errors = {}
        for variable, value in vars( self ).items():
            if variable not in variables_to_skip:
                if 'age' not in variable: #need to skip age-based variables because they can be a range+NA.  Age ranges are checked in different function
                    accepted_values = self.dictionary.loc[ variable ].data_values
                    if accepted_values: ##if a variable that has at least one given data value in dict
                        if str( value ) not in accepted_values:
                            data_value_errors[ variable ] = f"'{ value }' is NOT a valid value for { variable }"
                
        if data_value_errors:
            # self.data_errors [ 'accepted_values_check' ] = '; '.join( [ f"{ x[ 0 ] }: { x[ 1 ] }" for x in data_value_errors.items() ] )
            self.data_errors [ 'accepted_values_check' ] = '; '.join( [ f"{ x[ 1 ] }" for x in data_value_errors.items() ] )


    def ad_check( self ):
        if self.ad == 1:
            if not ( self.incad == 1 or self.prevad == 1 ):
                self.data_errors[ "ad_check" ] = "AD has value of 1 but 0 values for incad and prevad. "
            else:
                if self.incad == 1 and self.prevad == 1:
                    self.data_errors[ "ad_check" ] = "AD has value of 1 but both incad and prevad have 1 values"

        else:
            if not ( self.incad == 0 and self.prevad == 0 ):
                self.data_errors[ "ad_check" ] = "AD has value of 0 but 1 values in either incad or prevad"

    def ad_status_switch_check( self ):
        if self.ad == 0 and self.previous_ad == 1:
            self.data_errors[ "ad_case_to_control" ] = "Subject's AD status changed from case to control in update.  Please confirm."

    def prevad_age_baseline_check( self ):
        if self.prevad == 1:
            if self.age_baseline != 'NA':
                self.data_errors[ "prevad_age_baseline_check" ] = "Prevad value of 1 with non-NA age_baseline value."

    def braak_inc_prev_check( self ):
        if isinstance( self.braak, int ):
            if self.braak < 4:
                if not (self.incad == 0 and self.prevad == 0):
                    self.data_errors[ "braak_inc_prev_check" ] = "Braak score less than 4 but inc/prev_ad indicated."
            else:
                if not (self.incad == 1 or self.prevad == 1):
                    self.data_errors[ "braak_inc_prev_check" ] = "Braak score greater than 3 but no inc/prev_ad indicated."       
        else:
            self.data_errors[ "braak_na_check" ] = "Missing braak value, examine for the absence of neuropathological confirmation of AD status."
    
    def age_range_check( self, age_phenotype, value ):
        try:
            if int( value ) not in range( 121 ):
                self.data_errors[ f"{ age_phenotype }_range_check" ] = f"'{ value }' is NOT valid for { age_phenotype }"
        except:
            if value != 'NA':
                    ## make sure its not a case of a #+ (eg 90+) value given
                try:
                    if int( value.replace("+", "") ) not in range( 121 ):
                        self.data_errors[ f"{ age_phenotype }_range_check" ] = f"'{ value }' is NOT valid for { age_phenotype }"

                except:
                    self.data_errors[ f"{ age_phenotype }_range_check" ] = f"'{ value }' is NOT valid for { age_phenotype }"
        
    ## checks that only run on update-validation
    def update_age_check( self ):
        if self.age !='NA' and self.previous_age !='NA':
            if not self.age >= self.previous_age:
                self.data_errors[ "age_check" ] = "Age decreased between last release and update."
        else:
            if self.age == 'NA' and self.previous_age == 'NA':
                return
            else:
                if self.age != 'NA' and self.previous_age == 'NA':
                    self.data_errors[ "age_check" ] = "Previous age given as NA but update gives numerical value."

        if not self.age >= self.previous_age:
            self.data_errors[ "age_check" ] = "Age decreased between last release and update."

    def update_age_under_50_check( self ):        
        if self.age !='NA' and self.previous_age !='NA':
            if self.age < 50:
                self.data_errors[ "age_under_50_check" ] = "Subject's age is less than 50.  Please confirm samples."

class PSP_Subject:
    def __init__( self, subject_data, all_data, checktype ):
        
        self.subject_id = subject_id
        self.subject_type = 'PSP/CDB'
        self.dictionary = get_dict_data( database_connection( f"SELECT dictionary_name FROM env_var_by_subject_type WHERE subject_type = '{ self.subject_type }'" )[ 0 ][ 0 ] )
        self.race = subject_data[ "race" ]
        self.sex = subject_data[ "sex" ]
        self.diagnosis  = subject_data[ "diagnosis" ]
        self.ageonset = handle_age_values( subject_data[ "ageonset" ] )
        self.agedeath = handle_age_values( subject_data[ "agedeath" ] )

        if checktype == 'initial-validation':
            self.comments = subject_data[ "comments" ]
    
        if checktype == 'update-validation':
            self.subject_id = subject_data[ "subject_id" ]
            self.previous_comments = subject_data["prev_comments"]
            self.previous_race = subject_data[ "prev_race" ]
            self.previous_sex = subject_data[ "prev_sex" ]
            self.previous_diagnosis  = subject_data[ "prev_diagnosis" ]
            self.previous_ageonset = handle_age_values( subject_data[ "prev_ageonset" ] )
            self.previous_agedeath = handle_age_values( subject_data[ "prev_agedeath" ] )
        
        self.all_data = all_data

    def age_range_check( self, age_phenotype, value ):
        try:
            if int( value ) not in range( 121 ):
                self.data_errors[ f"{ age_phenotype }_range_check" ] = f"'{ value }' is NOT valid for { age_phenotype }"
        except:
            if value != 'NA':
                    ## make sure its not a case of a #+ (eg 90+) value given
                try:
                    if int( value.replace("+", "") ) not in range( 121 ):
                        self.data_errors[ f"{ age_phenotype }_range_check" ] = f"'{ value }' is NOT valid for { age_phenotype }"

                except:
                    self.data_errors[ f"{ age_phenotype }_range_check" ] = f"'{ value }' is NOT valid for { age_phenotype }"

    ## Initial validation checks ( no comparison data )
    def check_for_blank_values( self ):
        """enumerates over object properties, checks that all (excluding comments) have a given value"""  
        variables_to_skip = [ 'dictionary', 'all_data', 'data_errors' ] 
        blank_variable_list = []
        for variable, value in vars( self ).items():
            if variable not in variables_to_skip:
                if value == '' and variable.lower() != 'comments':
                    blank_variable_list.append( variable )
        
        if blank_variable_list:
            blank_var_string = ', '.join( blank_variable_list )
            self.data_errors [ 'blank_value_check' ] = f"One or more variables found with blank values: { blank_var_string }"

    def check_data_values_against_dictionary( self ):
        variables_to_skip = [ 'dictionary', 'all_data', 'data_errors' ]
        data_value_errors = {}
        for variable, value in vars( self ).items():
            if 'age' not in variable: #need to skip age-based variables because they can be a range+NA.  Age ranges are checked in different function
                accepted_values = self.dictionary.loc[ variable ].data_values
                if value not in accepted_values:
                    data_value_errors[ variable ] = f"{ value } is NOT a valid value for { variable }"
            
        if data_value_errors:
            self.data_errors [ 'accepted_values_check' ] = '; '.join( [ f"{ x[ 0 ] }: { x[ 1 ] }" for x in data_value_errors.items() ] )

    def run_initial_validation_checks( self ):
        self.check_for_blank_values()
        self.check_data_values_against_dictionary()
        self.age_range_check( "ageonset", self.ageonset )
        self.age_range_check( "agedeath", self.agedeath )

        return self.data_errors

    def run_update_validation_checks( self ):
        self.check_for_blank_values()
        self.check_data_values_against_dictionary()
        self.age_range_check( "ageonset", self.ageonset )
        self.age_range_check( "agedeath", self.agedeath )

        return self.data_errors

### Child classes of PSP and non-PSP Parent Classes
class Case_Control_Subject( Non_PSP_Subject ):
    def __init__( self, subject_id, subject_data, all_data, checktype ):
        super().__init__( subject_id, subject_data, all_data, checktype )
        
        self.subject_type = 'case/control'
        ## get the dictionary of appropriate variables and var-values from database
        self.dictionary = get_dict_data( database_connection( f"SELECT dictionary_name FROM env_var_by_subject_type WHERE subject_type = '{ self.subject_type }'" )[ 0 ][ 0 ] )
        self.ad = subject_data[ "ad" ]
        self.age = handle_age_values( subject_data[ "age" ] )
        self.incad = subject_data[ "incad" ]
        self.prevad = subject_data[ "prevad" ]
        self.selection = subject_data[ "selection" ]

        if checktype == 'update-validation':
            self.previous_ad = subject_data[ "prev_ad" ]
            self.previous_age = handle_age_values( subject_data[ "prev_age" ] )
            self.previous_incad = subject_data[ "prev_incad" ]
            self.previous_prevad = subject_data[ "prev_prevad" ]
            self.previous_selection = subject_data[ "prev_selection" ]

    ### functions that call the appropriate checks for initil validation and updates
    def run_initial_validation_checks( self ):
        self.check_for_blank_values()
        self.check_data_values_against_dictionary()
        self.age_range_check( "age", self.age )
        self.age_range_check( "age_baseline", self.age_baseline )

        return self.data_errors
        
    def run_update_validation_checks( self ):
        self.check_for_blank_values()
        self.update_age_check()
        self.ad_check()
        self.ad_status_switch_check()
        self.prevad_age_baseline_check()
        self.update_age_under_50_check()
        self.braak_inc_prev_check()

        return self.data_errors
        
class Family_Subject( Non_PSP_Subject ):
    def __init__( self, subject_id, subject_data, all_data, checktype ):
        super().__init__( subject_id, subject_data, all_data, checktype )

        self.subject_type = 'family'
        ## get the dictionary of appropriate variables and var-values from database
        self.dictionary = get_dict_data( database_connection( f"SELECT dictionary_name FROM env_var_by_subject_type WHERE subject_type = '{ self.subject_type }'" )[ 0 ][ 0 ] )
        self.ad = subject_data[ "ad" ]
        self.age = handle_age_values( subject_data[ "age" ] )
        self.famid = subject_data[ "famid" ]
        self.father = subject_data[ "father" ]
        self.mother = subject_data[ "mother" ]
        self.famgrp = subject_data[ "famgrp" ]

        if checktype == 'update-validation':
            self.previous_ad = subject_data[ "prev_ad" ]
            self.previous_age = handle_age_values( subject_data[ "prev_age" ] )
            self.previous_famid = subject_data[ "prev_famid" ]
            self.previous_father = subject_data[ "prev_father" ]
            self.previous_mother = subject_data[ "prev_mother" ]
            self.previous_famgrp = subject_data[ "prev_famgrp" ]


    def check_father_exists( self ):
        """check if father is 0, mother must be 0
            - if != 0, then mother cant be zero (and vice versa)
            - if is an id, check that theres an entry for the subject
        """
        if self.father == 0:
            if self.mother != 0:
                self.data_errors[ "father_check" ] = f"Subject's father_id is { self.father }, but mother_id is non-zero"
        else:
            if self.father not in self.all_data.keys():
                self.data_errors[ "father_check" ] = f"There is no subject entry for { self.father }, given as father of { self.subject_id }"
    
    def check_mother_exists( self ):
        if self.mother == 0:
            if self.father != 0:
                self.data_errors[ "mother_check" ] = f"Subject's mother_id is { self.mother }, but father_id is non-zero"
        else:
            if self.mother not in self.all_data.keys():
                self.data_errors[ "mother_check" ] = f"There is no subject entry for { self.mother }, given as mother of { self.subject_id }"

    def offspring_same_family_id_check( self, all_data_as_df ):
        """
        for an id identified as a mother/father, check that it's family_id is the same 
        between parents and offspring.  
        """
        try:
            mother_fam_id = all_data_as_df.loc[ self.mother ][ "famid" ]

            if mother_fam_id != self.famid:
                self.data_errors[ "mother_famid_check" ] = f"Subject's mother ( { self.mother } ) has different family_id ( { mother_fam_id } ) than subject."

        except:
            print( f"No record found for mother: { self.mother }. Skipping..." )
        
        try:
            father_fam_id = all_data_as_df.loc[ self.father ][ "famid" ]

            if father_fam_id != self.famid:
                self.data_errors[ "father_famid_check" ] = f"Subject's father ( { self.father } ) has different family_id ( { father_fam_id } ) than subject."
        except:
            print( f"No record found for father: { self.father }. Skipping..." )

    def check_all_family_same_famgrp( self, all_data_as_df ):
        """
        all subjects with same family_id have to have same fmgrp as well
        """
        family = all_data_as_df[ all_data_as_df[ 'famid' ] == self.famid ]
        famgrp_set = set( family[ 'famgrp' ] )

        if len( famgrp_set ) > 1:
            self.data_errors[ 'all_family_same_famgrp_check' ] = f"Members of subject's family ( { self.famid } ) have different famgrp values ( Values found: { ', '.join( f'{x}' for x in famgrp_set ) } )"
        
    def match_parent_sex( self, all_data_as_df ):
        """Check that record for given mother/father has correct sex given"""
        if self.mother != 0:
            """mother's sex must be '1' """
            if all_data_as_df.loc[self.mother]["sex"] != 1:
                self.data_errors[ 'mother_sex_match_check' ] = f"Subjects mother ( {self.mother} ) has mismatched sex designation."

        if self.father != 0:
            """father's sex must be '0' """
            if all_data_as_df.loc[self.father]["sex"] != 0:
                self.data_errors[ 'father_sex_match_check' ] = f"Subjects father ( {self.father} ) has mismatched sex designation."


    ### functions that call the appropriate checks for initil validation and updates
    def run_initial_validation_checks( self ):
        ## make a dataframe out of the all data so can do some pedigree-type checks across rows easily
        df = pd.read_json( json.dumps( self.all_data ) ).transpose()

        self.check_for_blank_values()
        self.check_data_values_against_dictionary()
        self.age_range_check( "age", self.age )
        self.age_range_check( "age_baseline", self.age_baseline )

        self.check_father_exists()
        self.check_mother_exists()

        if self.mother or self.father != 0:
            self.offspring_same_family_id_check( df )
            self.check_all_family_same_famgrp( df )
            self.match_parent_sex( df )

        return self.data_errors

    def run_update_validation_checks( self ):
        ## make a dataframe out of the all data so can do some pedigree-type checks across rows easily
        df = pd.read_json( json.dumps( self.all_data ) ).transpose()

        self.update_age_check()
        self.ad_status_switch_check()
        self.update_age_under_50_check()
        self.check_father_exists()
        self.check_mother_exists()

        if self.mother or self.father != 0:
            self.offspring_same_family_id_check( df )
            self.check_all_family_same_famgrp( df )
            self.match_parent_sex( df )

        return self.data_errors

class ADNI_Subject( Non_PSP_Subject ):
    def __init__( self, subject_id, subject_data, all_data, checktype ):
        super().__init__( subject_id, subject_data, all_data, checktype )

        self.subject_type = 'ADNI'
        self.dictionary = get_dict_data( database_connection( f"SELECT dictionary_name FROM env_var_by_subject_type WHERE subject_type = '{ self.subject_type }'" )[ 0 ][ 0 ] )
        self.ad_last_visit = subject_data[ "ad_last_visit" ]
        self.age_current = handle_age_values( subject_data[ "age_current" ] )
        self.incad = subject_data[ "incad" ]
        self.prevad = subject_data[ "prevad" ]
        self.age_ad_onset = handle_age_values( subject_data[ "age_ad_onset" ] )
        self.age_mci_onset = handle_age_values( subject_data[ "age_mci_onset" ] )
        self.mci_last_visit = subject_data[ "mci_last_visit" ]

        if checktype == 'update-validation':
            self.previous_ad_last_visit = subject_data[ "prev_ad_last_visit" ]
            self.previous_age_current = handle_age_values( subject_data[ "prev_age_current" ] )
            self.previous_incad = subject_data[ "prev_incad" ]
            self.previous_prevad = subject_data[ "prev_prevad" ]
            self.previous_age_ad_onset = handle_age_values( subject_data[ "prev_age_ad_onset" ] )
            self.previous_age_mci_onset = handle_age_values( subject_data[ "prev_age_mci_onset" ] )
            self.previous_mci_last_visit = subject_data[ "prev_mci_last_visit" ]

    def ad_check( self ):
        if self.ad_last_visit == 1:
            if not ( self.incad == 1 or self.prevad == 1 ):
                self.data_errors[ "ad_last_visit_check" ] = "AD has value of 1 but 0 values for incad and prevad. "
            else:
                if self.incad == 1 and self.prevad == 1:
                    self.data_errors[ "ad_last_visit_check" ] = "AD has value of 1 but both incad and prevad have 1 values"

        else:
            if not ( self.incad == 0 and self.prevad == 0 ):
                self.data_errors[ "ad_last_visit_check" ] = "AD has value of 0 but 1 values in either incad or prevad"

    def mci_no_inc_prev_ad_check( self ):
        if self.mci_last_visit == 1:
            if self.prevad == 1:
                self.data_errors['mci_no_prevad_check'] = "Subject as mci value of 1 but also prev_ad value of 1."\

            if self.incad == 1:
                self.data_errors['mci_no_incad_check'] = "Subject as mci value of 1 but also inc_ad value of 1."\

            if self.ad_last_visit == 1:
                self.data_errors['mci_no_ad_check'] = "Subject as mci value of 1 but also AD value of 1."\
                
    def diagnosis_onset_age_check( self ):
        """
        checks that entry has age_onset value matching 
        indication of AD/MCI AND doesn't have age_onset value for the other
        """
        if self.ad_last_visit == 1:
            if self.age_ad_onset == 'NA':
                self.data_errors[ 'ad_has_onset_age_check' ] = "Subject has AD value of 1 but no age_ad_onset value."

            if self.age_mci_onset != 'NA':
                self.data_errors[ 'ad_has_no_mci_onset_age_check' ] = "Subject ahs AD value of 1 but has value for mci_onset_age"

        if self.mci_last_visit == 1:
            if self.age_mci_onset == 'NA':
                self.data_errors[ 'mci_has_onset_age_check' ] = "Subject has MCI value of 1 but no ad_mci_onset value."
    
            if self.age_ad_onset != 'NA':
                self.data_errors[ 'mci_has_no_ad_onset_age_check' ] = "Subject has MCI value of 1 but has value for ad_onset_age"

    def both_ad_and_mci_check( self ):
        if self.ad_last_visit == 1:
            if self.mci_last_visit == 1:
                self.data_errors[ 'both_ad_and_mci_check' ] = "Subject has both AD and MCI values of 1."
    
    ## checks that only run for update validation ( ie. compare update and previous )
    def update_age_check( self ):
        if self.age_current !='NA' and self.previous_age_current !='NA':
            if not self.age_current >= self.previous_age_current:
                self.data_errors[ "age_check" ] = "Age decreased between last release and update."
        else:
            if self.age_current == 'NA' and self.previous_age_current == 'NA':
                return
            else:
                if self.age_current != 'NA' and self.previous_age_current == 'NA':
                    self.data_errors[ "age_check" ] = "Previous age given as NA but update gives numerical value."

    def update_age_under_50_check( self ):
        if self.age_current !='NA' and self.previous_age_current !='NA':
            if self.age_current < 50:
                self.data_errors[ "age_under_50_check" ] = "Subject's age is less than 50.  Please confirm samples."

    def ad_status_switch_check( self ):
        if self.ad_last_visit == 0 and self.previous_ad_last_visit == 1:
            self.data_errors[ "ad_last_visit_case_to_control" ] = "Subject's AD status changed from case to control in update.  Please confirm."

    def mci_status_switch_check( self ):
        if self.mci_last_visit == 0 and self.previous_mci_last_visit == 1:
            self.data_errors[ "mci_last_visit_case_to_control" ] = "Subject's MCI status changed from case to control in update.  Please confirm."

    ### functions that call the appropriate checks for initil validation and updates
    def run_initial_validation_checks( self ):
        self.check_for_blank_values()
        self.check_data_values_against_dictionary()
        self.age_range_check( "age_current", self.age_current )
        self.age_range_check( "age_baseline", self.age_baseline )
        self.age_range_check( "age_ad_onset", self.age_ad_onset )
        self.age_range_check( "mci_ad_onset", self.mci_ad_onset )

        self.ad_check()
        self.mci_no_inc_prev_ad_check()
        self.diagnosis_onset_age_check()
        self.both_ad_and_mci_check()

        return self.data_errors

    def run_update_validation_checks( self ):
        self.update_age_check()
        self.update_age_under_50_check()
        self.ad_status_switch_check()
        self.mci_status_switch_check()
        
        self.ad_check()
        self.mci_no_inc_prev_ad_check()
        self.diagnosis_onset_age_check()  ##checks both that AD/MCI indication and onset_age match, and that doesn't have age_onsent value for the other
        self.both_ad_and_mci_check()
        
        return self.data_errors



