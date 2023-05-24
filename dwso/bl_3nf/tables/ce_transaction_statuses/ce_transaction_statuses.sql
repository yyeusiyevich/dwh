CREATE TABLE IF NOT EXISTS bl_3nf.ce_transaction_statuses
  (
     status_id     		INTEGER PRIMARY KEY,
     status_src_id 		TEXT NOT NULL,
     source_system      TEXT NOT NULL,
     source_entity      TEXT NOT NULL,
     status_name  		TEXT NOT NULL,
     insert_dt          TIMESTAMP WITH TIME ZONE NOT NULL,
     update_dt          TIMESTAMP WITH TIME ZONE NOT NULL
  ); 