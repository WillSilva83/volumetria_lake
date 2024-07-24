import boto3 
from src.utils.logging import config_logger

logger = config_logger('App Tables S3 X Athena')

class Glue():
    def __init__(self) -> None:
        pass

    def get_list_tables(self, database:str, glue = boto3.client('glue'), region = 'sa-east-1') -> list:
        ''' List tables from database 
        Returns: list {Table_Name, Location, TableType}  
        '''

        output_list = []
        
        try:
            response = glue.get_tables(DatabaseName=database, MaxResults=100)
            tables_list = response['TableList']
            
            while 'NextToken' in response:
                next_token = response['NextToken']
                response = glue.get_tables(DatabaseName=database, NextToken=next_token, MaxResults=200)
                tables_list.extend(response['TableList'])

                for table in tables_list:
                    dict_itens = {}
                    
                    try: 
                    
                        dict_itens["Table_Name"] = table["Name"]
                        
                        # Adicionado para padronizar
                        if not table["StorageDescriptor"]["Location"].endswith('/'):
                            table["StorageDescriptor"]["Location"] += '/'
                        
                        dict_itens["Location"] = table["StorageDescriptor"]["Location"]
                        dict_itens["TableType"] = table["TableType"]
                        dict_itens["IsVerify"] = False
                        output_list.append(dict_itens)
                    
                    except Exception as e: 
                        logger.warning("Tabela " + table["Name"] + " não foi adicionada.")
                        
                        dict_itens["Table_Error"] = table["Name"]
                        dict_itens["Location"] = ""
                        dict_itens["IsVerify"] = False
                        output_list.append(dict_itens)
                    

        except Exception as e:
            print(f"Erro ao retornar tabelas do Database: {database}. Erro: {e}")


        return output_list
    


