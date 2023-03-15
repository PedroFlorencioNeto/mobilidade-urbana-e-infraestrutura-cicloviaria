import os
import pandas as pd

def read_sinistros_data(folder):
    """
    Função que realiza a leitura dos arquivos de sinistros desagregados por ano
    em formato CSV e retorna um DataFrame contendo as colunas:
    - Ano, Automovel, Motocicleta, Bicicleta, Mortos, Feridos e Ilesos.

    A pasta deve conter apenas dados anuais em formato CSV seguindo as regras:
    - Sinistros_YYYY.xls

    *YYYY representa o ano dos dados adquiridos no sistema Plataforma Vida da AMC.

    Args:
        folder (str): string com o nome da pasta no Google Drive em que a pasta está armazenada.

    Returns:
        df_final (DataFrame): Pandas Dataframe com as colunas especificadas acima.
    """
    list_of_files = os.listdir(folder)
    df_final = pd.DataFrame(columns=['Automovel','Motocicleta','Bicicleta','Outros','Mortos','Feridos','Ilesos'])

    for file in list_of_files:
        path = folder+file
        file_year = os.path.basename(path).split('_')[1][:4]
        
        if file.__contains__('.csv'):
            df = pd.read_csv(path, sep=';', encoding='UTF-8', usecols = ['AUTOMOVEL','MOTOCICLETA','BICICLETA',
                                                                          'CAMINHAO','MICROONIBUS','CICLOMOTOR',
                                                                          'TREM','MORTOS','FERIDOS','ILESOS'])
        
        else:
            df = pd.read_csv(path, sep=';', encoding='ISO-8859-1', usecols = ['AUTOMOVEL','MOTOCICLETA','BICICLETA',
                                                                          'CAMINHAO','MICROONIBUS','CICLOMOTOR',
                                                                          'TREM','MORTOS','FERIDOS','ILESOS'])
        df = df.sum()
        consolidado_por_ano = {'Ano':file_year, 'Automovel':df['AUTOMOVEL'], 'Motocicleta':df['MOTOCICLETA'],
                              'Bicicleta':df['BICICLETA'], 'Outros': (df['CAMINHAO']+df['MICROONIBUS']+df['CICLOMOTOR']+df['TREM']),
                              'Mortos':df['MORTOS'],'Feridos':df['FERIDOS'], 'Ilesos':df['ILESOS']}
        
        df_final = df_final.append(consolidado_por_ano, ignore_index=True)
    
    return df_final

# Caminho para a pasta que contém os dados de Sinistros
folder = 'C:\\Users\\pedroneto\\Documents\\Frente e Verso Infraestrutura Cicloviária\\mobilidade-urbana-e-infraestrutura-cicloviaria\\data\\sinistros\\'

# Exportando CSV com os dados tratados
read_sinistros_data(folder).to_csv('SinitrosHistoricoVeiculoSeveridade.csv', encoding = 'utf-8')
