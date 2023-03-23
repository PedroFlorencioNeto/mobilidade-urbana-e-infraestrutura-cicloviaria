import pandas as pd
from logging import info
from prefect import task
from configparser import ConfigParser

@task(name="Extracao de dados", description="Extrai dados contidos em um bucket S3")
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

    print(config['aws_boto_credentials']['access_key'])


    info('Os dados foram baixados corretamente!')
    return raw_data

