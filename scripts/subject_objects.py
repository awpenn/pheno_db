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
        self.previous_selection = data[ "prev_selection" ]\

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
