import boto3
import numpy as np
import pandas as pd
from io import StringIO
from logging import info
from prefect import task
from configparser import ConfigParser

# Pipeline de Dados ETL - Arquitetura Delta Lake

# (E)xtração de Dados de Bucket S3;
# (T)ransformação de dados para o formato Pandas DataFrame;
# (L)oading (Carregamento [PT-BR]) de dados nas camadas Silver e Gold.

@task(name="Extract", description="Extrai dados contidos em um bucket S3")
def extract_data(bucket_name: str, folder_name: str) -> list:
    '''
    Função que acessa arquivos (formato xls ou xlsx) de sinistros de trânsito 
    contidos em um bucket S3 da AWS e retorna um pandas DataFrame.

    A pasta no bucket deve conter apenas dados anuais em formato CSV seguindo as regras:
    - Sinistros_YYYY.xls

    * YYYY representa o ano dos dados filtrados através da Plataforma Vida da AMC.

    Args:
        bucket_name (str): string com o nome do bucket no S3.
        folder_name (str): string com o nome da pasta no bucket em que os dados de sinistros estão.

    Returns:
        list_ objects (list): lista com os todos os arquivos de sinistros coletados por ano.
    '''    

    # Acessando credenciais da AWS
    config = ConfigParser()
    config.read('pipeline.conf')
    ACCESS_KEY = config.get('aws_boto_credentials', 'access_key')
    SECRET_KEY = config.get('aws_boto_credentials', 'secret_key')

    # Acessando os dados contidos no bucket especificado
    s3_resource = boto3.resource('s3', aws_access_key_id = ACCESS_KEY, aws_secret_access_key = SECRET_KEY)
    Bucket= s3_resource.Bucket(bucket_name)
    list_objects = []

    for object in Bucket.objects.filter(Prefix=folder_name):
        if object.key != folder_name:
            list_objects.append(object.key)

    info('200: Acesso ao bucket S3 bem-sucedido!')

    return list_objects

@task(name='Transform', description='Transforma em DataFrame a lista recebida na task Extract')
def transform_data(list_objects: list, bucket_name: str) -> pd.DataFrame:
    """
    Função que recebe a lista de arquivos de sinistros desagregados por ano e
    transforma em um DataFrame único com os dados agregados em série histórica.

    Args:
        list_objects (list):Lista de arquivos de um bucket S3
    
    Returns:
        df (pd.DataFrame): DataFrame do Pandas com os dados agregados
    """
    config = ConfigParser()
    config.read('pipeline.conf')
    ACCESS_KEY = config.get('aws_boto_credentials', 'access_key')
    SECRET_KEY = config.get('aws_boto_credentials', 'secret_key')
    s3_client = boto3.client("s3", aws_access_key_id = ACCESS_KEY, aws_secret_access_key = SECRET_KEY)
    df_temp = pd.DataFrame()

    for object in list_objects:
        response = s3_client.get_object(Bucket=bucket_name, Key=object)
        df = pd.read_csv(StringIO(response['Body'].read().decode('ISO-8859-1')),sep=';')
        df = df.sum()
        consolidado_por_ano = {'Automovel':df['AUTOMOVEL'],
                                'Motocicleta':df['MOTOCICLETA'],
                                'Bicicleta':df['BICICLETA'],
                                'Outros': (df['CAMINHAO']+df['MICROONIBUS']+df['CICLOMOTOR']+df['TREM']),
                                'Mortos':df['MORTOS'],
                                'Feridos':df['FERIDOS'],
                                'Ilesos':df['ILESOS']}
        df_temp = df_temp.append(consolidado_por_ano, ignore_index=True)
    
    df_temp['Ano'] = np.arange(2015,2020,1)
    df = df_temp

    info("200: Dados historicos de sinistros de bicicletas processados!")

    return df

@task(name='Load', description='Carrega os dados recebidos da task Transform nas camadas Silver ou Gold')
def load_data(df:pd.DataFrame) -> None:
    """
    Função que salva o DataFrame em um arquivo CSV e carrega na camada Silver ou Gold
    Args:
        df (pd.DataFrame): DataFrame com os dados processados;
    
    Returns:
        None
    """

    # Camada Silver
    path_data = '../mobilidade-urbana-e-infraestrutura-cicloviaria/data/'
    df.to_csv(path_data+"silver/sinistros-2015-2019.csv", encoding='utf-8', index = False)


    info("200: Dados exportados em CSV!")
