CREATE OR REPLACE FUNCTION get_dyno() RETURN TABLE(subject_id text, _data jsonb) AS $$
	BEGIN
		CREATE TABLE public.temp(
			subject_id text,
			_data jsonb
		);

		do
		$do$
		declare
			i text;
		begin
			for i in (select distinct subject_id from ds_subjects_phenotypes)
			loop
			insert into temp(subject_id, _data)
			select subject_id, _data from ds_subjects_phenotypes where subject_id = i and _data->>'data_version' = (select max(_data->>'data_version') from ds_subjects_phenotypes where subject_id = i);


			end loop;
		end;
		$do$;
		RETURN temp;
	END;
$$ LANGUAGE plpgsql;

/*get the entry with highest published vers_no for a particular subject_id*/
CREATE OR REPLACE FUNCTION get_current_published_dyno() RETURNS TABLE
(id int, subject_id char varying, _data jsonb, published boolean) AS $$
	declare
		i RECORD;
	BEGIN
		for i in (select distinct ds_subjects_phenotypes.subject_id, max(ds_subjects_phenotypes._data->>'data_version') as dv from ds_subjects_phenotypes where ds_subjects_phenotypes.published = TRUE GROUP BY ds_subjects_phenotypes.subject_id)
			loop
				return query select ds_subjects_phenotypes.id, ds_subjects_phenotypes.subject_id, ds_subjects_phenotypes._data, ds_subjects_phenotypes.published from ds_subjects_phenotypes where ds_subjects_phenotypes.subject_id = i.subject_id and ds_subjects_phenotypes._data->>'data_version' = i.dv;
			end loop;
	END;
$$ LANGUAGE plpgsql;

/*get the entry with highest vers_no for a particular subject_id, regardless of publication status*/
CREATE OR REPLACE FUNCTION get_newest_dyno() RETURNS TABLE
(id int, subject_id char varying, _data jsonb, published boolean) AS $$
	declare
		i RECORD;
	BEGIN
		for i in (select distinct ds_subjects_phenotypes.subject_id, max(ds_subjects_phenotypes._data->>'data_version') as dv from ds_subjects_phenotypes GROUP BY ds_subjects_phenotypes.subject_id)
			loop
				return query select ds_subjects_phenotypes.id, ds_subjects_phenotypes.subject_id, ds_subjects_phenotypes._data, ds_subjects_phenotypes.published from ds_subjects_phenotypes where ds_subjects_phenotypes.subject_id = i.subject_id and ds_subjects_phenotypes._data->>'data_version' = i.dv;
			end loop;
	END;
$$ LANGUAGE plpgsql;

/*Only return entries that for subject_id with highest data_version number AND are unpublished*/
CREATE OR REPLACE FUNCTION get_updates_dyno() RETURNS TABLE
(id int, subject_id char varying, _data jsonb, published boolean) AS $$
	declare
		i RECORD;
	BEGIN
		for i in (select distinct ds_subjects_phenotypes.subject_id, max(ds_subjects_phenotypes._data->>'data_version') as dv from ds_subjects_phenotypes GROUP BY ds_subjects_phenotypes.subject_id)
			loop
				return query select ds_subjects_phenotypes.id, ds_subjects_phenotypes.subject_id, ds_subjects_phenotypes._data, ds_subjects_phenotypes.published from ds_subjects_phenotypes where ds_subjects_phenotypes.subject_id = i.subject_id and ds_subjects_phenotypes._data->>'data_version' = i.dv AND ds_subjects_phenotypes.published = FALSE;
			end loop;
	END;
$$ LANGUAGE plpgsql;