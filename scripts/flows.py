from prefect import flow
from prefect.task_runners import SequentialTaskRunner
from tasks import extract_data, transform_data, load_data

@flow(name='ETL de sinistros de bicicletas', task_runner=SequentialTaskRunner())
def etl_sinistros_bicicletas(bucket: str, folder: str):

    #Tasks
    list_objects = extract_data.submit('diobs-frente-verso-ciclomobilidade', 'Sinistros/')
    df = transform_data.submit(list_objects, bucket)
    load_data.submit(df)

if __name__ == '__main__':
    etl_sinistros_bicicletas('diobs-frente-verso-ciclomobilidade', 'Sinistros/') 