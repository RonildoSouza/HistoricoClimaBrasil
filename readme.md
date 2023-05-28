Raw Data Zone
Uma dos principais diferencias de Data Lakes para Warehouses, já que nessa zona pode-se armazenar com agilidade todos os dados de fontes relevantes, independente de quanto será consumido de imediato.

Como estes dados são brutos, ainda não receberam os tratamentos necessários para serem consumidos em análises tradicionais, mas agregam muito valor por fornecerem aos cientistas de dados uma fonte crua, a partir da qual podem criar suas próprias modelagens para machine learning e AI.

Trusted Zone
Dados alocados nesta zona já foram tratados, sofreram as transformações necessárias para serem consumidos e já possuem garantias de Data Quality, podendo ser considerados exatos e confiáveis.

Refined Zone
Na Refined Zone é possível encontrar dados tratados e enriquecidos, estando prontos para serem consumidos por aplicações externas.

Justamente por esse uso, essa camada costuma ser construída com infraestrutura de bancos de dados relacionais (SQL Server, Oracle, etc.), facilitando a conexão com API’s e sistemas transacionais.

-----

https://github.com/neylsoncrepalde/pucminas-data-pipelines/blob/main/sparkcode/titanic_example_delta.py

https://github.com/kelvins/Municipios-Brasileiros/raw/main/csv/estados.csv
https://github.com/kelvins/Municipios-Brasileiros/raw/main/csv/municipios.csv

https://github.com/tbrugz/geodata-br