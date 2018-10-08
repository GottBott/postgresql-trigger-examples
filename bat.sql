-- after generating the uuid the front end should store it
--  so it can come back to user transaction at a later date
 create
	table
		nabat.user_bulk_transaction( id serial primary key,
		user_email varchar(255),
		transaction_uuid UUID unique,
		created_date timestamp default current_timestamp );

create
	index user_bulk_transaction_email_idx on
	nabat.user_bulk_transaction (user_email);
--Broad Habitat Type	Audio Recording Name (*.wav *.zc)	Software Type	Auto Id	manual Id
-- table has same datatypes as csv 
-- using actual values not forign key ids
-- aka no refrence value checking on front end 
-- except for validation 
 create
	table
		nabat.bulk_sae ( id serial primary key,
		transaction_uuid UUID not null references nabat.user_bulk_transaction(transaction_uuid),
		project_id integer,
		grts_id integer,
		site_name varchar(255),
		latitude float,
		longitude float,
		activation_start_time timestamptz,
		activation_end_time timestamptz,
		detector varchar(255),
		microphone varchar(255),
		microphone_orientation varchar(3),
		microphone_height float,
		distance_to_clutter float,
		clutter_type varchar(255),
		distance_to_water float,
		water_type varchar(255),
		precent_clutter integer,
		habitat_type varchar(255),
		audio_recording_name varchar(255),
		software_type varchar(255),
		auto_id varchar(5),
		manual_id varchar(5),
		error boolean default false,
		error_text text );

create
	index bulk_sae_transaction_idx on
	nabat.bulk_sae (transaction_uuid);
-- view to help see summary info at transaction level
 create
or replace
view nabat.grouped_bulk_sae_view as select
	sae.transaction_uuid,
	ut.user_email,
	sum(case when sae.error then 1 else 0 end) as failed,
	sum(case when sae.error then 0 else 1 end) as passed
from
	nabat.bulk_sae as sae
join nabat.user_bulk_transaction as ut on
	sae.transaction_uuid = ut.transaction_uuid
group by
	sae.transaction_uuid,
	ut.user_email;
-- function to break apart the csv row
-- and normilize it into tables
 create
or replace
function bulk_insert_sae_row () returns trigger as $trigger$ declare surveyId integer;

eventId integer;

valueId integer;
begin
-- check survey exists, if not make one
 select
	id into
		surveyId
	from
		nabat.survey
	where
		project_id = new.project_id
		and grts_id = new.grts_id;

if surveyId > 0 then perform 1;
else insert
	into
		nabat.survey (project_id,
		grts_id,
		date)
	values (new.project_id,
	new.grts_id,
	current_date);

select
	id into
		surveyId
	from
		nabat.survey
	where
		project_id = new.project_id
		and grts_id = new.grts_id;
end if;
-- check event exists, if not make one
 select
	id into
		eventId
	from
		nabat.sae
	where
		site_name = new.site_name
		and activation_start_time = new.activation_start_time
		and activation_end_time = new.activation_end_time
		and survey_id = surveyId;

if eventId > 0 then perform 1;
else insert
	into
		nabat.sae (site_name,
		activation_start_time,
		activation_end_time,
		survey_id)
	values (new.site_name,
	new.activation_start_time,
	new.activation_end_time,
	surveyId);

select
	id into
		eventId
	from
		nabat.sae
	where
		name = new.event_name
		and survey_id = surveyId;
end if;
-- check value exists, if not make one
 select
	id into
		valueId
	from
		nabat.saev
	where
		audio_recording_name = new.audio_recording_name
		and event_id = eventId;

if valueId > 0 then perform 1;
else insert
	into
		nabat.saev (event_id,
		audio_recording_name,
		software_type,
		auto_id,
		manual_id)
	values (eventId,
	new.audio_recording_name,
	new.software_type,
	(
	select
		id
	from
		nabat.bat
	where
		sppcode = new.auto_id),
	(
	select
		id
	from
		nabat.bat
	where
		sppcode = new.manual_id) );
end if;

return new;
-- handel exception such as failed look up of bat id
 exception
when others then update
	nabat.bulk_sae set
		error = true,
		error_text = 'Error =' || SQLERRM || sqlstate
	where
		id = new.id;

return new;
end;

$trigger$ language plpgsql;
-- after insert into staging table 
-- try to spit the data and insert it
 create
	trigger bulk_insert_sae after insert
		on
		nabat.bulk_sae for each row execute procedure bulk_insert_sae_row();
