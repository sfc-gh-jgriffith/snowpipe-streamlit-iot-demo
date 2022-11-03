import snowflake.connector
import sys
import os

from time import sleep

from random import randrange

time_between_calls = int(sys.argv[1])
min_sensors = int(sys.argv[2])
max_sensors = int(sys.argv[3])


conn = snowflake.connector.connect(
        user=os.getenv("SFUSER"),
        password=os.getenv("SFPASSWORD"),
        account=os.getenv("ACCOUNT"),
        role="ACCOUNTADMIN",
        warehouse="XSMALL",
        database="IOT_DEMO",
        schema="PUBLIC"
    )

while True:
    with conn.cursor() as cur:
        r_num = randrange(min_sensors, max_sensors)
        resp = cur.execute(f"call generate_iot_data({r_num}, ['TEMP','SPEED','VIBRATION'], [1.0, 2.0, 3.0], [0.5, .22, 1.0], '@iot_stage', 1, 15)")
        print(resp)
    sleep(time_between_calls)