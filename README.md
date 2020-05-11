# Postgres ETL example
This ETL loads over 1TB of reddit comments pulled from reddit. Each file is read and extracted, processed, and then loaded into a Postgres database. The postgres database exhibits a one-to-one relationship, where the primary key is the parent_id in the initial comment table, and the foreign key is the parent_id of the comment replies found in the replies table.



