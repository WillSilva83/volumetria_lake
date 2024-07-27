from src.classes.Glue import Glue
from src.classes.S3 import S3
from src.utils.external_libs import write_file, validate_string
from src.utils.logging import config_logger


logger = config_logger('App Tables S3 X Athena')
client_s3 = S3()
client_glue = Glue()
'''

Volume 
    Verificar os maiores volumes que não estão sendo mais carregadas
    Maiores volumes que estão sendo carregadas diáriamente

Escopo 

1 - A tabela com alto volume está sendo carregada todos os dias?
2 - É uma carga full diária? 
3 - Qual o custo dessas tabela de alto volume?

'''
path_file_s3 = "arquivo_s3.txt"
path_file_tables_glue = "arquivo_glue.txt"
bucket_name = "bucket_teste"


def merge_data_s3_x_glue():
    #    for tabela in tabelas_glue:
    #    if tabela['Location'] in diretorios_s3:
    #        # Remove o item do S3 não sendo necessário uma nova validação
    #        diretorios_s3.remove(tabela['Location'])
    #        
    #        # Adiciona o ponto de validação da Tabela Glue 
    #        tabela['IsVerify'] = True
    pass

def extract_tables_glue(database_name: str, file_name: str):
    '''
        Funcao para a chamada da extracao das tabelas do Glue
    '''
    try:
        tabelas_glue = client_glue.get_list_tables(database_name)
        if tabelas_glue == []:
            
            logger.info("Gravando extração de tabelas do Glue.")
            write_file(tabelas_glue, file_name = file_name, is_json = True, encoding = 'utf-8', ensure_ascii = False, indent = 4)
        else:
            logger.info("Sem retorno de Dados do Database: " + database_name + ".")

    except Exception as e:
        logger.error("Erro ao gravar o arquivo. Error: {e}")
        
    pass 

def extract_s3(bucket_name: str, file_name: str, is_format_output: bool, is_json : bool):
    '''
        Funcao para realizar a chamada da extracao do bucket S3.
    '''
    try:
        logger.info("Gravando extração de tabelas do Glue.")
        diretorios_s3 = client_s3.get_list_dir(bucket_name = bucket_name, is_format_output = is_format_output)
        write_file(diretorios_s3, file_name = file_name, is_json = False)

    except Exception as e: 
        logger.error("Erro ao gravar o arquivo de extração do S3. Error: {e}")
        
    pass

def extract_data_from_aws(service, bucket = None, ): 
    ''' Extrai dados da aws '''

    if bucket is None:
        print("Error")

    if service == 'S3':
         diretorios_s3 = client_s3.get_list_dir("bucket-tz-destino-teste", is_format_output = True)

    elif service == 'Glue':
        tabelas_glue = client_glue.get_list_tables("database_test")
        for tabela in tabelas_glue:
            logger.info(f"Verificando Tabela: {tabela['Table_Name']}")
            if tabela['Location'] in diretorios_s3 and not tabela['IsVerify']:
                # Remove o item do S3 não sendo necessário uma nova validação
                diretorios_s3.remove(tabela['Location'])

                # Adiciona o ponto de validação da Tabela Glue 
                tabela['IsVerify'] = True


def run(path_file_s3 = None, path_file_tables_glue = None, bucket_name = "" ) -> bool: 

    '''
        Funcao Principal para Extracao e Comparacao de Bases 
    '''
    if path_file_s3 or path_file_tables_glue:
        logger.info("Informados os arquivos: S3:" + path_file_s3 + ". Athena: " + path_file_tables_glue + ".")
        logger.info("Não será necessário a extração novamente.")
    else: 
        # Extrair dados 
        extract_tables()



if __name__ == '__main__': 
    '''
        Funcao para chamada dos processo. 
    '''


    if run(path_file_s3 = path_file_s3, 
           path_file_tables_glue = path_file_tables_glue, 
           bucket_name = bucket_name
        ):
        logger.info("Execucao realizada com sucesso.")
    else: 
        logger.warning("Erro na execução da validação de base.")
    