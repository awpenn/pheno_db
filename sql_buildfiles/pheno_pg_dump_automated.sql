# everyday, dump contents of pheno database into bkps dir
0 8 * * * pg_dump -w production_pheno_database > /var/lib/postgresql/11/bkps/pheno_bkps/production_pheno_database_`date +\%d\%m\%y\%H\%M\%S`.sql

# everyday, delete files older than 30 days in pheno_bkps directory
0 3 * * * find /var/lib/postgresql/11/bkps/pheno_bkps/ -type f -mtime +364 -delete