-- updated at column trigger
CREATE FUNCTION update_updated_at_column() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
  BEGIN
    NEW.updatedat = NOW();
    RETURN NEW;
  END;
$$;

CREATE TRIGGER subject_phenotypes_updated_at_modtime BEFORE UPDATE ON ds_subjects_phenotypes FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

CREATE TRIGGER subject_phenotypes_updated_at_modtime BEFORE UPDATE ON ds_subjects_phenotypes_baseline FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();


