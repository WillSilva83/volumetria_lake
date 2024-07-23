from classes.Glue import Glue
from classes.S3 import S3
from utils.external_libs import write_file
from utils.logging import config_logger


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


def run(execution_on = True, path_file_s3 = None, path_file_tables = None):

    if execution_on:
        if path_file_s3 or path_file_tables:
            logger.error("Informe os diretorios")
        else:
            logger.info("Fluxo para remoção") 
    else:
        diretorios_s3 = {'s3://bucket-tz-destino-teste/table2_test/', 's3://bucket-tz-destino-teste/projeto/arquivo/', 's3://bucket-tz-destino-teste/table1_test/', 's3://bucket-tz-destino-teste/projeto/'}
        tabelas_glue = [{'Table_Name': 'table1_test', 'Location': 's3://bucket-tz-destino-teste/table1_test/', 'TableType': 'EXTERNAL_TABLE', 'IsVerify': False}, {'Table_Name': 'table_test_2', 'Location': 's3://bucket-tz-destino-teste/table2_test/', 'TableType': 'EXTERNAL_TABLE', 'IsVerify': False}]

        for tabela in tabelas_glue:
            logger.info(f"Verificando Tabela: {tabela['Table_Name']}")
            if tabela['Location'] in diretorios_s3 and not tabela['IsVerify']:
                # Remove o item do S3 não sendo necessário uma nova validação
                diretorios_s3.remove(tabela['Location'])

                # Adiciona o ponto de validação da Tabela Glue 
                tabela['IsVerify'] = True

        write_file(diretorios_s3, file_name = "diretorios_s3.txt", is_json = False)
        write_file(tabelas_glue, file_name = "tabelas_glue.json", is_json = True, encoding = 'utf-8', ensure_ascii = False, indent = 4)


    pass 


if __name__ == '__main__': 
    '''
        
    
    ''' 

    run(execution_on = False)
    


    # Retornar as tabelas do meu database 
        # Essas tabelas tem seu destino S3
        # Faço o batimento entre a Lista de tabelas com os diretórios listados 
        # Caso não bata o diretório é sem filho
        # Para evitar diversas requisições é necessário salvar o progresso 
    #

    #= True)
    #








    #print(f"diretorio do S3: {diretorios_s3}")
    #print(f"tabelas do Glue: {tabelas_glue}")

    #dict_output = {}
