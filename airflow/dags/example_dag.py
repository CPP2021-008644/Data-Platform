import asyncio
import pendulum

from airflow.decorators import task, dag as dagd
import pandas as pd
from datetime import datetime as dt, timedelta, time
import random

from src.bdptplt import DataPlatformDB


DAG_ID = "example_dag"
TAGS = ["insert", "dummy", "postgres"]
START_DATE = pendulum.datetime(2024, 6, 5, 8, 0, tz="Europe/Madrid")
CATCHUP = False
SCHEDULE_INTERVAL = "0 8 * * *"

@dagd(
    dag_id=DAG_ID,
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    catchup=CATCHUP,
    tags=TAGS,
)
def example_dag():
    @task
    def create_dummy_df(n, contract_name):
        start_date = dt.combine(dt.now().date(), time(9, 0))  # Market opens at 9:00 AM

        # Generate random datetime values during the trading day
        dtime = [start_date + timedelta(seconds=random.randint(0, 32400)) for _ in range(n)]
        dtime.sort()  # Optional: sort by time

        # Simulate a starting price and small random walk (to mimic market noise)
        price = [100 + random.gauss(0, 0.5)]  # start around 100 with small volatility
        for _ in range(1, n):
            price.append(price[-1] + random.gauss(0, 0.5))  # continue random walk

        # Simulate trade sizes (e.g., integers from 1 to 1000 shares/contracts)
        size = [random.randint(1, 1000) for _ in range(n)]

        # Create DataFrame
        df = pd.DataFrame({
            'dtime': dtime,
            'contract_id': range(1, n + 1),  # Unique contract IDs
            'contract_name' : contract_name,
            'price': price,
            'size': size,
        })
        return df.copy()

    @task
    def insert_dummy_data(df):
        db = DataPlatformDB() 
        db.upserter(df=df, tabtype="prob", instrument='PRB')
        
    df = create_dummy_df(100, 'dummy_contract')
    insert_dummy_data(df)


example_dag()
