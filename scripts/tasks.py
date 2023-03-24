import boto3
import pandas as pd
from logging import info
from prefect import task
from configparser import ConfigParser

#@task(name="Extracao de dados", description="Extrai dados contidos em um bucket S3")
def extract_data(bucket_name: str, folder_name: str) -> str:
    '''
    Função que realiza o download dos arquivos (formato xls ou xlsx) de sinistros de trânsito 
    contidos em um bucket S3 da AWS e retorna um texto em formato CSV.

    A pasta no bucket deve conter apenas dados anuais em formato CSV seguindo as regras:
    - Sinistros_YYYY.xls

    * YYYY representa o ano dos dados adquiridos no sistema Plataforma Vida da AMC.

    Args:
        folder (str): string com o nome da pasta no Google Drive em que a pasta está armazenada.

    Returns:
        str: Texto em formato CSV.
    '''    

    # Acessando credenciais da AWS
    config = ConfigParser()
    config.read('pipeline.conf')
    ACCESS_KEY = config['aws_boto_credentials']['access_key']
    SECRET_KEY = config['aws_boto_credentials']['secret_key']
    
    # Acessando os dados contidos no bucket especificado
    s3_resource = boto3.resource('s3', aws_access_key_id=ACCESS_KEY, aws_secret_key_id=SECRET_KEY)
    Bucket = s3_resource.Bucket(bucket_name)
  
    for object in Bucket.objects.filter(Prefix=folder_name):
        print(object.key)
    info('Os dados foram baixados corretamente!')

    return "Pedro"

extract_data("diobs-frente-verso-ciclomobilidade", "Sinistros/")

@task
def transform_data():

@task
def load_data(dataFrame: pd.DataFrame) ->

