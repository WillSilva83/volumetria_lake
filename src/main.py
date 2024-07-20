from utils.Athena_obj import AthenaObj


'''

Volume 
    Verificar os maiores volumes que não estão sendo mais carregadas
    Maiores volumes que estão sendo carregadas diáriamente

Escopo 

1 - A tabela com alto volume está sendo carregada todos os dias?
2 - É uma carga full diária? 
3 - Qual o custo dessas tabela de alto volume?

'''
    
athena = AthenaObj()

database = "DB_HUB_BDDIPRD1_RAW_PRD"
table = "TB_CASH_SALDOS_TESTE_N"
type = "volume"

query_test = athena.prepare_query(database, table, type)

print(query_test)