CREATE TABLE IF NOT EXISTS bl_cl.logging (
	log_id serial4 NOT NULL,
	user_name TEXT NOT NULL,
	event_time TIMESTAMPTZ NOT NULL,
	schema_name TEXT NULL,
	table_name TEXT NULL,
	command_name TEXT NULL,
	rows_affected INT4 NULL,
	error_type TEXT NULL,
	error_message TEXT NULL,
	CONSTRAINT logging_pkey PRIMARY KEY (log_id)
);