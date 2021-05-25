"""
Beginning point in updated workflow to begin a new release.
- create new release in versions table
- copy over previous latest published release subjects into new release
"""

import sys
sys.path.append('/home/pheno_db/.venv/lib/python3.6/site-packages/')

import csv
from dotenv import load_dotenv
import os
import json
import datetime

import flagchecks
import pheno_utils

script_name = 'create_new_release.py'

def main( ):
    pass
    ## 1. Create new version 
    ##      i. make sure not a dupe
    ##      ii. add to table and get new dvid
    ## 2. Get subject type
    ## 3. Copy subjects from previous release to new one
    ##      i. get latest published version no from view
    ##      ii. get records from ds table with that version
    ##      iii. update data version in json
    ##      iv. run flags
    ##      v. create new unpublished records in ds-table


if __name__ == '__main__':
    main(  )