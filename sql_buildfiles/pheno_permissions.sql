--PREVENT TABLE DROPS FOR NONSUPERUSER
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

--BASIC PERMISSIONS FOR MANAGER ROLE
-- GRANT INSERT, SELECT, UPDATE ON TABLE [] TO manager;

GRANT INSERT, SELECT, UPDATE ON TABLE data_versions TO manager;
GRANT INSERT, SELECT, UPDATE ON TABLE ds_subjects_phenotypes TO manager;
GRANT INSERT, SELECT, UPDATE ON TABLE ds_subjects_phenotypes_baseline TO manager;
GRANT INSERT, SELECT, UPDATE ON TABLE data_dictionaries TO manager;
GRANT INSERT, SELECT, UPDATE ON TABLE env_var_by_subject_type TO manager;

-- GRANT SELECT ON public.[view] to manager;
GRANT SELECT ON public._get_current_data TO manager;
GRANT SELECT ON public._get_unpublished_updates_data TO manager;
GRANT SELECT ON public._get_newest_data TO manager;
GRANT SELECT ON public.cc_all TO manager;
GRANT SELECT ON public.cc_current TO manager;
GRANT SELECT ON public.cc_unpublished_updates TO manager;
GRANT SELECT ON public.cc_newest TO manager;
GRANT SELECT ON public.cc_baseline TO manager;
GRANT SELECT ON public.fam_all TO manager;
GRANT SELECT ON public.fam_current TO manager;
GRANT SELECT ON public.fam_unpublished_updates TO manager;
GRANT SELECT ON public.fam_newest TO manager;
GRANT SELECT ON public.fam_baseline TO manager;
GRANT SELECT ON public.adni_all TO manager;
GRANT SELECT ON public.adni_current TO manager;
GRANT SELECT ON public.adni_unpublished_updates TO manager;
GRANT SELECT ON public.adni_newest TO manager;
GRANT SELECT ON public.adni_baseline TO manager;
GRANT SELECT ON public.psp_cdb_all TO manager;
GRANT SELECT ON public.psp_cdb_current TO manager;
GRANT SELECT ON public.psp_cdb_unpublished_updates TO manager;
GRANT SELECT ON public.psp_cdb_newest TO manager;
GRANT SELECT ON public.psp_cdb_baseline TO manager;

--MANAGER USAGE FOR SEQUENCES
-- GRANT USAGE ON SEQUENCE [].alias_ids_id_seq TO manager;
GRANT USAGE ON SEQUENCE public.data_versions_id_seq TO manager;
GRANT USAGE ON SEQUENCE public.ds_subjects_phenotypes_id_seq TO manager;
GRANT USAGE ON SEQUENCE public.ds_subjects_phenotypes_baseline_id_seq TO manager;
GRANT USAGE ON SEQUENCE public.data_dictionaries_id_seq TO manager;
GRANT USAGE ON SEQUENCE public.env_var_by_subject_type_id_seq TO manager;



--PERMISSIONS FOR VIEWER ROLE
GRANT SELECT ON TABLE data_versions TO viewer;
GRANT SELECT ON TABLE ds_subjects_phenotypes TO viewer;
GRANT SELECT ON TABLE ds_subjects_phenotypes_baseline TO viewer;
GRANT SELECT ON TABLE data_dictionaries TO viewer;
GRANT SELECT ON TABLE env_var_by_subject_type TO viewer;

GRANT SELECT ON public._get_current_data TO viewer;
GRANT SELECT ON public._get_unpublished_updates_data TO viewer;
GRANT SELECT ON public._get_newest_data TO viewer;
GRANT SELECT ON public.cc_all TO viewer;
GRANT SELECT ON public.cc_current TO viewer;
GRANT SELECT ON public.cc_unpublished_updates TO viewer;
GRANT SELECT ON public.cc_newest TO viewer;
GRANT SELECT ON public.cc_baseline TO viewer;
GRANT SELECT ON public.fam_all TO viewer;
GRANT SELECT ON public.fam_current TO viewer;
GRANT SELECT ON public.fam_unpublished_updates TO viewer;
GRANT SELECT ON public.fam_newest TO viewer;
GRANT SELECT ON public.fam_baseline TO viewer;
GRANT SELECT ON public.adni_all TO viewer;
GRANT SELECT ON public.adni_current TO viewer;
GRANT SELECT ON public.adni_unpublished_updates TO viewer;
GRANT SELECT ON public.adni_newest TO viewer;
GRANT SELECT ON public.adni_baseline TO viewer;
GRANT SELECT ON public.psp_cdb_all TO viewer;
GRANT SELECT ON public.psp_cdb_current TO viewer;
GRANT SELECT ON public.psp_cdb_unpublished_updates TO viewer;
GRANT SELECT ON public.psp_cdb_newest TO viewer;
GRANT SELECT ON public.psp_cdb_baseline TO viewer;

--
--