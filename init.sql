
DROP TRIGGER if exists bulk_insert ON nabat.bulk;
drop table if exists nabat.bulk;
DROP FUNCTION bulk_insert_row();
   
drop table nabat.value;
drop table nabat.event;
drop table nabat.survey;
drop table nabat.bat;


CREATE TABLE nabat.bat (
    id SERIAL PRIMARY KEY,
    name TEXT,
    sppcode TEXT
);


CREATE TABLE nabat.survey (
    id SERIAL PRIMARY KEY,
    name TEXT,
    description TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE nabat.survey IS
'grts';

CREATE TABLE nabat.event (
    id SERIAL PRIMARY KEY,
    name TEXT,
    description TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    survey_id INTEGER NOT NULL REFERENCES nabat.survey(id)
);

COMMENT ON TABLE nabat.event IS
'event meta data';


CREATE TABLE nabat.value (
    id SERIAL PRIMARY KEY,
    name TEXT,
    description TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    event_id INTEGER NOT NULL REFERENCES nabat.event(id),
    bat_id INTEGER NOT NULL REFERENCES nabat.bat(id)

);


INSERT INTO nabat.bat (name, sppcode) VALUES
('Big Brown Bat', 'BBB'),
('Small Red Bat', 'SRB'),
('Fuzzy Little Guy', 'FLG'),
('Killer Vampire Bat', 'KVB');


INSERT INTO nabat.survey (name, description) VALUES
('a', 'description 1'),
('b', 'description 2'),
('c', 'description 3');

INSERT INTO nabat.event (name, description, survey_id) VALUES
('d', 'Child description 1', 1),
('e', 'Child description 2', 2),
('f', 'Child description 3', 3),
('g', 'Child description 1', 1),
('h', 'Child description 2', 2),
('i', 'Child description 3', 3);


INSERT INTO nabat.value (name, description, event_id,bat_id) VALUES
('j', 'Child description 1', 1,1),
('k', 'Child description 2', 1,1),
('l', 'Child description 3', 2,1),
('m', 'Child description 1', 2,1),
('n', 'Child description 2', 3,1);


CREATE TABLE nabat.bulk (
    id SERIAL PRIMARY KEY,
    transaction_uuid UUID,
    survey_name TEXT,
    survey_description TEXT,
    event_name TEXT,
    event_description TEXT,
    value_name TEXT,
    value_description TEXT,
    value_bat_sppcode TEXT,
    error  BOOLEAN DEFAULT false,
    error_text TEXT
);

CREATE INDEX transaction_idx ON nabat.bulk (transaction_uuid);



CREATE OR REPLACE FUNCTION bulk_insert_row ()
RETURNS trigger AS $trigger$
declare
	surveyId integer;
	eventId integer;
	valueId integer;
BEGIN
   SELECT id into surveyId FROM nabat.survey where name =new.survey_name;
  IF surveyId > 0 THEN
    PERFORM 1;
ELSE
    insert into nabat.survey (name,description) values (new.survey_name,new.survey_description);
    SELECT id into surveyId FROM nabat.survey where name =new.survey_name;
END IF;

   SELECT id into eventId FROM nabat.event where name =new.event_name and survey_id = surveyId;
  IF eventId > 0 THEN
    PERFORM 1;
ELSE
    insert into nabat.event (name,description,survey_id) values (new.event_name,new.event_description,surveyId);
    SELECT id into eventId FROM nabat.event where name =new.event_name and survey_id = surveyId;
END IF;

   SELECT id into valueId FROM nabat.value where name =new.value_name and event_id = eventId;
  IF valueId > 0 THEN
    PERFORM 1;
else
    insert into nabat.value (name,description,event_id,bat_id) values 
    (new.value_name,new.value_description,eventId,(select id from nabat.bat where sppcode = new.value_bat_sppcode));
END IF;
  RETURN NEW;
EXCEPTION WHEN OTHERS then
UPDATE nabat.bulk SET error = true, error_text = 'Error =' || SQLERRM || SQLSTATE WHERE id = new.id;
   RETURN NEW;
END;
$trigger$ LANGUAGE plpgsql;


CREATE TRIGGER bulk_insert
    after INSERT ON nabat.bulk
    FOR EACH ROW
    EXECUTE PROCEDURE bulk_insert_row();

-- a good insert case
insert into nabat.bulk ( transaction_uuid,survey_name,survey_description,event_name,
event_description,value_name,value_description,value_bat_sppcode ) values
('0e37df36-f698-11e6-8dd4-cb9ced3df976','new survey 1','a survey','new event 1','an event','new value 1','a value','KVB');

-- insert with invalid spp code
insert into nabat.bulk ( transaction_uuid,survey_name,survey_description,event_name,
event_description,value_name,value_description,value_bat_sppcode ) values
('0e37df36-f698-11e6-8dd4-cb9ced3df976','new survey 1','a survey','new event 1','an event','new value 2','a value','XXXsa');