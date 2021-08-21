from airflow.decorators import dag, task, task_group
from airflow.utils.dates import days_ago


default_args = {
    'start_date': days_ago(1),
}


@dag(schedule_interval=None, default_args=default_args, catchup=False)
def EXAMPLE_taskgroups():

    # The initial task just sets the initial value
    @task
    def init():
        return 0

    # This task group has three subtasks:
    # each subtask will perform an operation on the initial value
    # this group will return a list with all the values of the subtasks
    @task_group(group_id='group_1')
    def group_1(value):

        # The @tasks below can be defined outside function `group_1`
        # What matters is where they are referenced
        @task(task_id='subtask_1')
        def task_1(value):
            task_1_result = value + 1
            return task_1_result

        @task(task_id='subtask_2')
        def task_2(value):
            task_2_result = value + 2
            return task_2_result

        @task(task_id='subtask_3')
        def task_3(value):
            task_3_result = value + 3
            return task_3_result

        # tasks are referenced here
        task_1_result = task_1(value)
        task_2_result = task_2(value)
        task_3_result = task_3(value)

        return [task_1_result, task_2_result, task_3_result]

    @task_group(group_id='group_2')
    def group_2(list):

        @task(task_id='subtask_4')
        def task_4(values):
            return sum(values)

        @task(task_id='subtask_5')
        def task_5(value):
            return value*2

        # task_4 will sum the values of the list sent by group_1
        # task_5 will multiply it by two.
        task_5_result = task_5(task_4(list))

        return task_5_result

    @task
    def end(value):
        print(f'this is the end: {value}')

    return end(group_2(group_1(init())))


dag = EXAMPLE_taskgroups()
