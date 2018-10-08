
--CREATE DATABASE nabat;
--GRANT ALL PRIVILEGES ON DATABASE nabat TO postgres;
--CREATE SCHEMA nabat;


DROP TRIGGER if exists bulk_insert ON nabat.bulk_sae;
drop view if exists nabat.grouped_bulk_sae_view;
drop table if exists nabat.bulk_sae;
DROP FUNCTION bulk_insert_sae_row();
   
drop table nabat.user_bulk_transaction;
drop table nabat.saev;
drop table nabat.sae;
drop table nabat.survey;
drop table nabat.bat;


-- refrence table 
CREATE TABLE nabat.bat (
    id SERIAL PRIMARY KEY,
    name TEXT,
    sppcode TEXT
);


-- survey with grts 
CREATE TABLE nabat.survey (
    id SERIAL PRIMARY KEY,
    name TEXT,
    description TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- stationary acoustic event
CREATE TABLE nabat.sae (
    id SERIAL PRIMARY KEY,
    name TEXT,
    description TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    survey_id INTEGER NOT NULL REFERENCES nabat.survey(id)
);


CREATE TABLE nabat.saev (
    id SERIAL PRIMARY KEY,
    name TEXT,
    description TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    event_id INTEGER NOT NULL REFERENCES nabat.sae(id),
    bat_id INTEGER NOT NULL REFERENCES nabat.bat(id)
);


INSERT INTO nabat.bat (name, sppcode) VALUES
('Big Brown Bat', 'BBB'),
('Small Red Bat', 'SRB'),
('Fuzzy Little Guy', 'FLG'),
('Killer Vampire Bat', 'KVB');


--INSERT INTO nabat.survey (name, description) VALUES
--('a', 'description 1'),
--('b', 'description 2'),
--('c', 'description 3');

--INSERT INTO nabat.sae (name, description, survey_id) VALUES
--('d', 'Child description 1', 1),
--('e', 'Child description 2', 2),
--('f', 'Child description 3', 3),
--('g', 'Child description 1', 1),
--('h', 'Child description 2', 2),
--('i', 'Child description 3', 3);


--INSERT INTO nabat.saev (name, description, event_id,bat_id) VALUES
--('j', 'Child description 1', 1,1),
--('k', 'Child description 2', 1,1),
--('l', 'Child description 3', 2,1),
--('m', 'Child description 1', 2,1),
--('n', 'Child description 2', 3,1);


-- table has same datatypes as csv 
-- using actual values not forign key ids
-- aka no refrence value checking on front end 
-- except for validation 
CREATE TABLE nabat.bulk_sae (
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

CREATE INDEX bulk_sae_transaction_idx ON nabat.bulk_sae (transaction_uuid);

-- view to help see summary info at transaction level
create or replace view nabat.grouped_bulk_sae_view as
	select transaction_uuid,
		sum(case when error then 1 else 0 end) as failed, 
		sum(case when error then 0 else 1 end) as passed
	from  nabat.bulk_sae
	group by transaction_uuid;


-- after generating the uuid the front end should store it
--  so it can come back to user transaction at a later date
create table nabat.user_bulk_transaction(
 id SERIAL PRIMARY KEY,
 user_email varchar(255),
 transaction_uuid UUID,
 created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX user_bulk_transaction_email_idx ON nabat.user_bulk_transaction (user_email);

-- function to break apart the csv row
-- and normilize it into tables
CREATE OR REPLACE FUNCTION bulk_insert_sae_row ()
RETURNS trigger AS $trigger$
declare
	surveyId integer;
	eventId integer;
	valueId integer;
begin
	-- check survey exists, if not make one
   SELECT id into surveyId FROM nabat.survey where name =new.survey_name;
  IF surveyId > 0 THEN
    PERFORM 1;
ELSE
    insert into nabat.survey (name,description) values (new.survey_name,new.survey_description);
    SELECT id into surveyId FROM nabat.survey where name =new.survey_name;
END IF;
	-- check event exists, if not make one
   SELECT id into eventId FROM nabat.sae where name =new.event_name and survey_id = surveyId;
  IF eventId > 0 THEN
    PERFORM 1;
ELSE
    insert into nabat.sae (name,description,survey_id) values (new.event_name,new.event_description,surveyId);
    SELECT id into eventId FROM nabat.sae where name =new.event_name and survey_id = surveyId;
END IF;
	-- check value exists, if not make one
   SELECT id into valueId FROM nabat.saev where name =new.value_name and event_id = eventId;
  IF valueId > 0 THEN
    PERFORM 1;
else
    insert into nabat.saev (name,description,event_id,bat_id) values 
    (new.value_name,new.value_description,eventId,(select id from nabat.bat where sppcode = new.value_bat_sppcode));
END IF;
  RETURN NEW;
 -- handel exception such as failed look up of bat id
EXCEPTION WHEN OTHERS then
UPDATE nabat.bulk_sae SET error = true, error_text = 'Error =' || SQLERRM || SQLSTATE WHERE id = new.id;
   RETURN NEW;
END;
$trigger$ LANGUAGE plpgsql;


-- after insert into staging table 
-- try to spit the data and insert it
CREATE TRIGGER bulk_insert_sae
    after INSERT ON nabat.bulk_sae
    FOR EACH ROW
    EXECUTE PROCEDURE bulk_insert_sae_row();

   
   
-- FIRST TRANACTION    
   
-- a good insert case
insert into nabat.bulk_sae ( transaction_uuid,survey_name,survey_description,event_name,
event_description,value_name,value_description,value_bat_sppcode ) values
('0e37df36-f698-11e6-8dd4-cb9ced3df976','Survey 1','This is a survey.','Event 1','This is an event.','Value 1','This is a value.','KVB');

insert into nabat.bulk_sae ( transaction_uuid,survey_name,survey_description,event_name,
event_description,value_name,value_description,value_bat_sppcode ) values
('0e37df36-f698-11e6-8dd4-cb9ced3df976','Survey 1','This is a survey.','Event 2','This is an event.','Value 2','This is a value.','BBB');

insert into nabat.bulk_sae ( transaction_uuid,survey_name,survey_description,event_name,
event_description,value_name,value_description,value_bat_sppcode ) values
('0e37df36-f698-11e6-8dd4-cb9ced3df976','Survey 1','This is a survey.','Event 2','This is an event.','Value 3','This is a value.','SRB');

-- insert with invalid spp code
insert into nabat.bulk_sae ( transaction_uuid,survey_name,survey_description,event_name,
event_description,value_name,value_description,value_bat_sppcode ) values
('0e37df36-f698-11e6-8dd4-cb9ced3df976','Survey 1','This is a survey.','Event 2','This is an event.','Value 4','This is a value.','XXX');


-- SECOND TRANACTION    

-- a good insert case
insert into nabat.bulk_sae ( transaction_uuid,survey_name,survey_description,event_name,
event_description,value_name,value_description,value_bat_sppcode ) values
('123e4567-e89b-12d3-a456-426655440000','Survey 1','This is a survey.','Event 2','This is an event.','Value 5','This is a value.','KVB');

-- insert with invalid spp code
insert into nabat.bulk_sae ( transaction_uuid,survey_name,survey_description,event_name,
event_description,value_name,value_description,value_bat_sppcode ) values
('123e4567-e89b-12d3-a456-426655440000','Survey 2','This is a survey.','Event 3','This is an event.','Value 6','This is a value.','XYZ');

-- insert with invalid spp code
insert into nabat.bulk_sae ( transaction_uuid,survey_name,survey_description,event_name,
event_description,value_name,value_description,value_bat_sppcode ) values
('123e4567-e89b-12d3-a456-426655440000','Survey 2','This is a survey.','Event 3','This is an event.','Value 7','This is a value.','QQQ');

-- insert with invalid spp code
insert into nabat.bulk_sae ( transaction_uuid,survey_name,survey_description,event_name,
event_description,value_name,value_description,value_bat_sppcode ) values
('123e4567-e89b-12d3-a456-426655440000','Survey 2','This is a survey.','Event 4','This is an event.','Value 8','This is a value.','BBB');


