# Snowpipe IOT Streaming Analytics


## Create database and stage in your Snowflake account
Set up your database and stage for your S3 data. Keeping the database called iot_demo and the sage called iot_stage will limit the number of modifications you need to make to the rest of the code.
```
create or replace database iot_demo;

use schema iot_demo.public;

create or replace stage iot_stage url = 's3://your-s3-location/' 
    storage_integration = your_s3_integration;
```

## Add Snowflake Authentiation Info Environemnt Variables
Replace the values in quotes with your Snowflake credentials.
```
    export SFUSER='SNOWFLAKEUSER'
    export SFPASSWORD='SNOWFLAKEPASSWORD'
    export ACCOUNT='org.alias'
    export ROLE='SYSADMIN'
```

## Create snowpipe objects
Run [SPROC_GENERATE_IOT_DATA.py](SPROC_GENERATE_IOT_DATA.py) with your database name and stage name as arguments.

This script creates a table, a pipe, and a stored procedure for generating synthetic IOT data.
```
python SPROC_GENERATE_IOT_DATA.py IOT_DEMO @IOT_STAGE
``` 
This creates a stored procedure with the following signature:
```
GENERATE_IOT_DATA(NUMBER, ARRAY, ARRAY, ARRAY, NUMBER, NUMBER, VARCHAR) RETURN VARCHAR
```
|SPROC Arg | Python Variable | Type | Description
|---|---|---|---|
|ARG1 | n_sensors  | int | Number of simulated IOT sensors to create data for| 
|ARG2 |  metric_names  | Array | Names of metrics to randomly generate | 
|ARG3 |  metric_means  | Array | Means values of metrics to randomly generate | 
|ARG4 |  metric_stddevs  | Array | Standard deviation values of metrics to randomly generate | 
|ARG5 |  stage_location  | varchar |  Stage location to put generated IOT data |
|ARG6 |  n_periods  | Array | Number observations to create per sensor. Total row count will be n_sensors * n_periods. Observations will have timestamps of every `seconds_between_observation` going backward from the current database timestamp for `n_periods`. Use the default value of `1` to generate one observation per sensor using the current database timestamp. 
|ARG7 |  seconds_between_observations  | Array | Number of seconds between observations | 

## Set up notifications for Snowpipe
Run `show stages` to get the `notification_channel` for your pipe. Add this to your S3 bucket to enable Snowpipe. 

## Run the stored procedure

You can now call the stored procedure from within SQL.
```
    call generate_iot_data(100, 
                           ['TEMP','SPEED','VIBRATION'], 
                           [1.0, 2.0, 3.0], 
                           [0.5, .22, 1.0], 
                           '@iot_stage',
                           1, 
                           15)
```
To simulate streaming data, run this locally on a loop in a python program (demonstrated in [generate_data_loop.py](generate_data_loop.py)) or schedule a Snowflake task to run at a specified interval. 

You must call [generate_data_loop.py](generate_data_loop.py) with an arguments of how many seconds you want between calls of the stored procedure, the minimum number of sensors, and the maximum number of sensors. A random number of sensors between the min and max will be created. For example, to call every 10 seconds and simulate between 1000 and 1500 sensors, you you would run:
```
    python generate_data_loop.py 10 1000 1500
```


## Create view using your specified metric names
use the variable names you specified when you call generate_iot_data to create a view
```
create or replace view iot_view as  
select 
    json['SENSOR_ID']::int as sensor_id,
    json['TEMP']::numeric(38,2) as AMPS,
    json['SPEED']::numeric(38,2)as t_celsius,
    json['VIBRATION']::numeric(38,2) as wind_speed_mph,
    timestamp
from iot_raw;
```

## Configure Streamlit
If you changed any of the metric names from what's included in this documentation, you'll need to modify the Streamlit app [iot_streamlit_app.py](iot_streamlit_app.py) to reflect your own variable names.

## Run Streamlit
Install Streamlit if you don't already have it installed and run your app.
```
    streamlit run iot_streamlit_app.py
```


