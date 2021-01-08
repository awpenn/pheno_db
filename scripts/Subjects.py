class Non_PSP_Subject():
    def __init__( self, data ):

        self.age_baseline = data[ "age_baseline" ]
        self.previous_age_baseline = data[ "prev_age_baseline" ]
        
        self.apoe = data[ "apoe" ]
        self.previous_apoe = data[ "prev_apoe" ]
        
        self.autopsy = data[ "autopsy" ]
        self.previous_autopsy = data[ "prev_autopsy" ]
        
        self.braak = data[ "braak" ]
        self.previous_braak = data[ "prev_braak" ]

        self.previous_comments = data["prev_comments"]
        
        self.ethnicity = data[ "ethnicity" ]
        self.previous_ethnicity = data[ "prev_ethnicity" ]
        
        self.race = data[ "race" ]
        self.previous_race = data[ "prev_race" ]
        
        self.sex = data[ "sex" ]
        self.previous_sex = data[ "prev_sex" ]

        self.data_errors = {}

    def age_check( self ):
        if not self.age >= self.previous_age:
            self.data_errors[ "age_check" ] = "Age decreased between last release and update."
    
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

    def age_under_50_check( self ):
        if self.age < 50:
            self.data_errors[ "age_under_50_check" ] = "Subject's age is less than 50.  Please confirm samples."

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

class Family_Subject( Non_PSP_Subject ):
    def __init__( self, data ):
        super().__init__( data )

        self.ad = data[ "ad" ]
        self.previous_ad = data[ "prev_ad" ]

        self.age = data[ "age" ]
        self.previous_age = data[ "prev_age" ]

        self.famid = data[ "famid" ]
        self.previous_famid = data[ "prev_famid" ]

        self.father = data[ "father" ]
        self.previous_father = data[ "prev_father" ]

        self.mother = data[ "mother" ]
        self.previous_mother = data[ "prev_mother" ]

        self.famgrp = data[ "famgrp" ]
        self.previous_famgrp = data[ "prev_famgrp" ]

        ##// add famid, mother/father check functions (or could add to parent)?

    def run_checks( self ):
        self.age_check()
        self.ad_status_switch_check()
        self.age_under_50_check()
        ##// add the id checks when written
        return self.data_errors

class Case_Control_Subject( Non_PSP_Subject ):
    def __init__( self, data ):
        super().__init__( data )
        
        self.ad = data[ "ad" ]
        self.previous_ad = data[ "prev_ad" ]

        self.age = data[ "age" ]
        self.previous_age = data[ "prev_age" ]

        self.incad = data[ "incad" ]
        self.previous_incad = data[ "prev_incad" ]

        self.prevad = data[ "prevad" ]
        self.previous_prevad = data[ "prev_prevad" ]

        self.selection = data[ "selection" ]
        self.previous_selection = data[ "prev_selection" ]
    
    def run_checks( self ):
        self.age_check()
        self.ad_check()
        self.ad_status_switch_check()
        self.prevad_age_baseline_check()
        self.age_under_50_check()
        self.braak_inc_prev_check()

        return self.data_errors
        
class ADNI_Subject( Non_PSP_Subject ):
    def __init__( self, data ):
        super().__init__( data )

        self.ad_last_visit = data[ "ad_last_visit" ]
        self.previous_ad_last_visit = data[ "prev_ad_last_visit" ]

        self.age_current = data[ "age_current" ]
        self.previous_age_current = data[ "prev_age_current" ]

        self.incad = data[ "incad" ]
        self.previous_incad = data[ "prev_incad" ]

        self.prevad = data[ "prevad" ]
        self.previous_prevad = data[ "prev_prevad" ]

        self.age_ad_onset = data[ "age_ad_onset" ]
        self.previous_age_ad_onset = data[ "prev_age_ad_onset" ]

        self.age_mci_onset = data[ "age_mci_onset" ]
        self.previous_age_mci_onset = data[ "prev_age_mci_onset" ]

        self.mci_last_visit = data[ "mci_last_visit" ]
        self.previous_mci_last_visit = data[ "prev_mci_last_visit" ]

class PSP_Subject():
    def __init__( self, data ):
        self.previous_comments = data["prev_comments"]

        self.race = data[ "race" ]
        self.previous_race = data[ "prev_race" ]

        self.sex = data[ "sex" ]
        self.previous_sex = data[ "prev_sex" ]

        self.diagnosis  = data[ "diagnosis" ]
        self.previous_diagnosis  = data[ "prev_diagnosis" ]

        self.ageonset = data[ "ageonset" ]
        self.previous_ageonset = data[ "prev_ageonset" ]

        self.agedeath = data[ "agedeath" ]
        self.previous_agedeath = data[ "prev_agedeath" ]
