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
GRANT SELECT ON public.get_all_cc to manager;
GRANT SELECT ON public.get_current_cc to manager;
GRANT SELECT ON public.get_newest_cc to manager;
GRANT SELECT ON public.get_unpublished_updates_cc to manager;
GRANT SELECT ON public.get_all_fam to manager;
GRANT SELECT ON public.get_current_fam to manager;
GRANT SELECT ON public.get_newest_fam to manager;
GRANT SELECT ON public.get_unpublished_updates_fam to manager;
GRANT SELECT ON public.get_current_and_baseline_cc to manager;
GRANT SELECT ON public.get_current_and_baseline_fam to manager;
GRANT SELECT ON public.get_all_adni to manager;
GRANT SELECT ON public.get_current_adni to manager;
GRANT SELECT ON public.get_newest_adni to manager;
GRANT SELECT ON public.get_unpublished_updates_adni to manager;
GRANT SELECT ON public.get_baseline_cc to manager;
GRANT SELECT ON public.get_baseline_fam to manager;


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

GRANT SELECT ON public.get_all_cc to viewer;
GRANT SELECT ON public.get_current_cc to viewer;
GRANT SELECT ON public.get_newest_cc to viewer;
GRANT SELECT ON public.get_unpublished_updates_cc to viewer;
GRANT SELECT ON public.get_all_fam to viewer;
GRANT SELECT ON public.get_current_fam to viewer;
GRANT SELECT ON public.get_newest_fam to viewer;
GRANT SELECT ON public.get_unpublished_updates_fam to viewer;
GRANT SELECT ON public.get_current_and_baseline_cc to viewer;
GRANT SELECT ON public.get_current_and_baseline_fam to viewer;
GRANT SELECT ON public.get_all_adni to viewer;
GRANT SELECT ON public.get_current_adni to viewer;
GRANT SELECT ON public.get_newest_adni to viewer;
GRANT SELECT ON public.get_unpublished_updates_adni to viewer;
GRANT SELECT ON public.get_baseline_cc to viewer;
GRANT SELECT ON public.get_baseline_fam to viewer;