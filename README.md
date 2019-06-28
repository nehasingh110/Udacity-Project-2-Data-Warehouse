### Purpose of the database
1. Sparkify, a music streaming startup, has grown their user base and song database and want to move their processes and data onto the cloud. 
2. The data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
3. I've built an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 


### Database schema design 
1. The schema contains a fact table called songplay that was populated from songplays_table pyspark sql dataframe which was obtained by joining df_s and df_l pyspark dataframes. This fact table contains some facts like start_time, level, location etc and few ID fields/keys that can be used to join this fact table to the dimension tables to get more information about artists, songs etc.
2. There are 4 dimension tables around 1 fact table in this star schema design.
3. The 4 dimension tables are users, song, artists and time. They contain data about users using the Sparkify, songs available in their databse, artists that song belongs to and the duration of songs respectively.
4. All these dimension tables have not null keys that can't be null since this key will be used in the join criteria's to join this table to the fact table.


### ETL Pipeline
1. The connection is set up by running create_tables.py that uses dwh.cfg to get the connection details like host, dbname, user, password and port.
2. After establishing a successful connection with Redshift cluster, a cursor is created to run the queries.
3. After that a function is called passing the cursor and connection details with it. The function executes all the drop table queries in a for loop. These drop table queries are present in sql_queries.py which is being called in create_tables.py.
4. Similarly the create table queries are executed by calling a function calledn create_tables and all the intermediate staging tables and fact and dimension tables get created.
5. After that etl.py is executed for the purpose of executing copy commands to populate the staging tables from S3 to Redshift and then to execute the insert queries to populated the fact and dimension tables from the staging tables.


### Steps to run files
1. Run create_tables.py in python console using the following command : %run create_tables.py
2. This will create all the empty staging, fact and dimension tables in the Redshift.
3. You may go to Amazon AWD and check if the tables got created in the public schema using Query Editor.
4. Once all the tables have been created run etl.py using command: %run etl.py
5. This will first populate the staging tables using copy command and will populate the fact and dimension tables from these staging tables.