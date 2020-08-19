/*View definitions for phenotype database*/
DROP VIEW IF EXISTS get_current_cc;

CREATE VIEW get_current_cc AS
select version_date, subjects_phenotypes_cc.* 
FROM subjects_phenotypes_cc 
JOIN data_versions
ON subjects_phenotypes_cc.data_version = data_versions.id
WHERE data_version 
    IN (
        select id from data_versions where version_date = (select max(version_date) from data_versions
    )
)


