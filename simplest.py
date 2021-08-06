from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from random import random

# Use the DAG decorator from Airflow
# `schedule_interval='@daily` means the DAG will run everyday at midnight.
# It's possible to set the schedule_interval to None (without quotes).
@dag(schedule_interval=None, start_date=days_ago(2), catchup=False)
# The function name will be the ID of the DAG.
# In this case it's called `EXAMPLE_simple`.
def EXAMPLE_simple():

    @task
    def task_1():
        # Generate a random number
        return random()

    @task
    def task_2(value):
        # Print the random number to the logs
        print(f'The randomly generated number is {value} .')

    # This will determine the direction of the tasks.
    # As you can see, task_2 runs after task_1 is done.
    # Task_2 then uses the result from task_1.
    return task_2(task_1())


dag = EXAMPLE_simple()
