# Comparativo entre S3 x Athena 

O script tem como objetivo realizar um batimento entre as tabelas criadas no AWS Glue e o que está presente no S3. 

Para melhor funcional e retorno é importante entender os particionamentos 


## Como usar 

- O script precisa apenas de alguns inputs 

### Caso seja a primeira extração
- É necessário informar o bucket (S3) para a extração.
- É necessário informar o Datababse (Glue) para a extração.


### Caso seja uma extração já realizada

- Informar o caminho do arquivo da exteação do S3.
- Informar o caminho do arquivo da extração do Athena.


## To do's 

- Aplicar arquivo de log 
- Aplicar 