# Pós-graduação Lato Sensu em Engenharia de Dados

O trabalho de conclusão de curso visou a implementação de uma infraestrutura de dados de baixo custo para empresas de pequeno e médio porte que não utilizam seus dados para tomadas de decisões por motivo de custo e/ou receio da real entrega de valor na análise dos dados.

Pipeline de dados com as ferramentas utilizadas para montar o ambiente para realizar a extração, transformação e carregamento dos dados de forma a possibilitar o uso das informações obtidas nas ferramentas analíticas.

<figure>
  <img
  src="https://github.com/user-attachments/assets/a93362c4-30f2-44db-b360-f666a3cb2488"
  alt="Pipeline de dados com ferramentas propostas pelo trabalho">
  <figcaption>Pipeline de dados com ferramentas propostas pelo trabalho</figcaption>
</figure>
</br>
</br>

Todas as ferramentas escolhidas podem ser utilizadas de forma gratuita e são totalmente escaláveis conforme a necessidade pois foram configuradas e provisionadas utilizando contêineres via Docker, desta forma a portabilidade das ferramentas para outros servidores e/ou o gerenciamento de recursos e realizado de forma simples e padronizada com os arquivos de configuração.

Os dados utilizados são um histórico do clima de municípios brasileiros, contendo dados de temperatura, umidade e velocidade do vento que foram extraídos do site [Open-Meteo](https://open-meteo.com/).

O ambiente de processamento e armazenamento dos dados foi configurado em um servidor do tipo VPS (Virtual Private Server) com sistema operacional Linux e hardware de 4 vCPUs, 16 GB de RAM e 200 GB de armazenamento, o custo do servidor virtual privado, em um período de 12 meses, pode variar entre R$ 700,00 e R$ 1.500,00 a depender da empresa de hospedagem.
