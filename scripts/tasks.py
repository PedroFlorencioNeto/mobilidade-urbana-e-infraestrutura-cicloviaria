import boto3
import numpy as np
import pandas as pd
from io import StringIO
from logging import info
from prefect import task
from configparser import ConfigParser

# Pipeline de Dados EtLT - Arquitetura Delta Lake

# (E)xtração de Dados de Bucket S3;
# (t)ransformação e agregação dos anos em Pandas DataFrame;
# (L)oad de dados no banco de dados Postgres e na pasta "Silver";
# (T)ransformação de dados para uso a nível de negócio e exportação para a pasta "Gold".

#@task(name="Extracao de dados", description="Extrai dados contidos em um bucket S3")
def extract_data(bucket_name: str, folder_name: str) -> pd.DataFrame:
    '''
    Função que realiza o download dos arquivos (formato xls ou xlsx) de sinistros de trânsito 
    contidos em um bucket S3 da AWS e retorna um pandas DataFrame.

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
    ACCESS_KEY = config.get('aws_boto_credentials', 'access_key')
    SECRET_KEY = config.get('aws_boto_credentials', 'secret_key')

    # Acessando os dados contidos no bucket especificado
    s3_client = boto3.client("s3", aws_access_key_id = ACCESS_KEY, aws_secret_access_key = SECRET_KEY)
    s3_resource = boto3.resource('s3', aws_access_key_id = ACCESS_KEY, aws_secret_access_key = SECRET_KEY)
    Bucket= s3_resource.Bucket(bucket_name)
    df_temp = pd.DataFrame()

    for object in Bucket.objects.filter(Prefix=folder_name):
        if object.key != folder_name:
            response = s3_client.get_object(Bucket=bucket_name, Key=object.key)
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

    info('Os dados foram baixados corretamente!')

    return df

bucket = "diobs-frente-verso-ciclomobilidade"
folder = "Sinistros/"
