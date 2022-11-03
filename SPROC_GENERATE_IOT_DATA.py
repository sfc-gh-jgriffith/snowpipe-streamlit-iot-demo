## SPROC_GENERATE_IOT_DATA.py
##
## Creates a stored procedure that generates simple IOT data and loads them to a stage location.
## Modify connection_parameters to set up your Snowflake connection

from snowflake.snowpark import Session
from snowflake.snowpark.functions import sproc
from snowflake.snowpark.types import ArrayType, IntegerType, StringType
import os
import sys



def create_snowpipe_objects(session: Session, stage_location: str):
    
    session.sql("create stage if not exists python_stage").collect()

    session.sql("""create table if not exists iot_raw (
                     json variant,
                     timestamp timestamp_tz)
                """).collect()
    
    session.sql(f"""
                   create file format if not exists json type='json'
                """).collect()
    
    session.sql(f"""
                   create or replace pipe iot_pipe 
                    auto_ingest=true 
                    as
                    copy into iot_raw 
                    from (select $1, 
                                 $1['OBSERVATION_TIMESTAMP']::timestamp_tz 
                          from {stage_location})
                    file_format=json;
                """).collect()


def register_stored_procedure(session: Session):
    @sproc(name = 'GENERATE_IOT_DATA'
           , is_permanent = True
           , replace = True
           , packages = ['snowflake-snowpark-python']
           , stage_location = '@python_stage')
    def generate_iot_data(session: Session, 
                        n_sensors: int, 
                        metric_names: list,
                        metric_means: list, 
                        metric_stddevs: list, 
                        stage_location: str,
                        n_periods: int, 
                        seconds_between_observations: int, 
                        ) -> str:
    
        metric_definitions = [{'metric_name':z[0], 'mean':z[1], 'stddev':z[2]} for z in zip(metric_names, metric_means, metric_stddevs)]

        metrics_sql = ','.join([f"round(NORMAL({m['mean']}, {m['stddev']}, RANDOM()), 4) as {m['metric_name']}" for m in metric_definitions] )

        tmst = str(session.sql("""select to_char(current_timestamp(), 'YYYYMMDDHHMISS')""").collect()[0][0])
        
        res = session.sql(f"""   
            copy into {stage_location}/{tmst}/ from (
                with 
                timestamp_generator as (
                    select 
                        timestampadd('seconds', 
                                -seq4()*{seconds_between_observations}, 
                                current_timestamp()) as timestamp
                        from 
                            table(generator(rowcount => {n_periods}))
                ),
                sensors as (
                    select 
                        seq4() as sensor_id
                    from 
                        table(generator(rowcount => {n_sensors}))
                ),
                obs as (
                select 
                    sensor_id,
                    {metrics_sql},
                    timestamp_generator.timestamp as observation_timestamp
                from 
                    sensors  
                    join timestamp_generator on true) 
                select 
                    OBJECT_CONSTRUCT(*) as json_value
                from 
                    obs 
            ) 
            file_format=(type=json) 
        """).collect()
        return "SUCCESS"

if __name__ == "__main__":

    database_name = sys.argv[1]
    stage_location = sys.argv[2]

    connection_parameters = {
        "user":os.getenv("SFUSER"),
        "password":os.getenv("SFPASSWORD"),
        "account":os.getenv("ACCOUNT"),
        "role":os.getenv("ROLE"),
        "warehouse":os.getenv("WAREHOUSE"),
    }
    
    session = Session.builder.configs(connection_parameters).create()

    try:
        session.sql(f"use database {database_name}").collect()
        session.sql(f"list {stage_location}").collect()
    
    except Exception as error:
        print("Please create database and stage before running.")
        print(error)

    finally:
        create_snowpipe_objects(session, stage_location)
        register_stored_procedure(session)
    
