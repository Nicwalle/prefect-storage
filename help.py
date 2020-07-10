import prefect
from prefect import task, Flow
from random import choice
from prefect.tasks.control_flow.case import case
from prefect.tasks.control_flow.conditional import merge
from time import sleep
from prefect.schedules.schedules import IntervalSchedule
from datetime import timedelta
from prefect import Client
from prefect.environments import LocalEnvironment
from prefect.environments import DaskKubernetesEnvironment
from prefect.environments.storage import GitHub

@task
def hello_task():
    log('Setting up tasks and waiting for 1s')
    sleep(1)

@task
def is_true_task() -> None:
    log('Choosing branch')
    return choice([True, False]) # Chooses a random boolean value

@task
def first_task() -> int:
    log('Executing first_task')
    return 1234

@task
def second_task() -> int:
    log('Executing second_task')
    return 5678

@task
def end_task(x: int):
    log('This is the end. Final value is {}'.format(x))

@task
def run_in_parrallel():
    log('Running concurrently')

def log(msg: str) -> None:
    logger = prefect.context.get("logger")
    logger.info(msg)

# environment = DaskKubernetesEnvironment(labels=["prefect-namespace"])


with Flow("Example Flow") as flow:
    hello = hello_task()
    cond = is_true_task()
    hello.set_downstream(cond) 


    with case(cond, True):
        val1 = first_task()

    with case(cond, False):
        val2 = second_task()

    parallel = run_in_parrallel()

    val = merge(val1, val2, parallel)
    
    end = end_task(val)

flow.storage = GitHub(repo="Nicwalle/prefect-storage", path="help.py")

flow.register(project_name="Test project 1", labels=["prefect-namespace"])

# 7b75f750949da928a35ebe28b0c9995589598bc7 
