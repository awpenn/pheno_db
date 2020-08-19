# everyday, dump contents of adspid database into bkps dir
0 8 * * * pg_dump -w production_adspid_database > /var/lib/postgresql/11/bkps/adspid_bkps/production_adspid_database_`date +\%d\%m\%y\%H\%M\%S`.sql

# everyday, delete files older than 30 days in adspid_bkps directory
0 3 * * * find /var/lib/postgresql/11/bkps/adspid_bkps/ -type f -mtime +364 -delete