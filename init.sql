CREATE DATABASE nabat;
GRANT ALL PRIVILEGES ON DATABASE nabat TO postgres;

CREATE SCHEMA nabat;

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
    event_id INTEGER NOT NULL REFERENCES nabat.event(id)
);


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


INSERT INTO nabat.value (name, description, event_id) VALUES
('j', 'Child description 1', 1),
('k', 'Child description 2', 1),
('l', 'Child description 3', 2),
('m', 'Child description 1', 2),
('n', 'Child description 2', 3),
('o', 'Child description 3', 3),
('p', 'Child description 1', 4),
('q', 'Child description 2', 4),
('r', 'Child description 3', 5),
('s', 'Child description 3', 5),
('t', 'Child description 3', 6),
('u', 'Child description 3', 6);


drop table if exists nabat.bulk;
CREATE TABLE nabat.bulk (
    id SERIAL PRIMARY KEY,
    survey_name TEXT,
    survey_description TEXT,
    event_name TEXT,
    event_description TEXT,
    value_name TEXT,
    value_description TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


DROP TRIGGER if exists bulk_insert ON nabat.bulk;
DROP FUNCTION bulk_insert_row();
   
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
ELSE
    insert into nabat.value (name,description,event_id) values (new.value_name,new.value_description,eventId);
    SELECT id into valueId FROM nabat.value where name =new.value_name and event_id = eventId;
END IF;

   RETURN NEW;
END;
$trigger$ LANGUAGE plpgsql;


CREATE TRIGGER bulk_insert
    after INSERT ON nabat.bulk
    FOR EACH ROW
    EXECUTE PROCEDURE bulk_insert_row();

insert into nabat.bulk ( survey_name,survey_description,event_name,
event_description,value_name,value_description ) values
('new survey 1','testing this','new event 2','testing this 1','new value 2','testing this 2');
