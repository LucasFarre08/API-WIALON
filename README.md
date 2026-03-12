API WIALON – Integração e Extração de Relatórios

Projeto desenvolvido em Python para integração com a API da Wialon, permitindo a execução de relatórios de telemetria e armazenamento dos dados em SQL Server ou MySQL.

A aplicação automatiza a coleta de dados como viagens, consumo de combustível, veículos e motoristas, facilitando análises operacionais e geração de relatórios.

Funcionalidades

Autenticação na API Wialon

Execução automática de relatórios

Extração de dados de telemetria

Processamento e tratamento de dados

Armazenamento em SQL Server ou MySQL

Exportação de relatórios

Sistema de logs de execução

Automação via scripts .bat

Tecnologias Utilizadas

Python 3.10+

Wialon Remote API

Pandas

Requests

PyODBC

Estrutura do Projeto
API-WIALON
│
├── dumps
├── .env.example
├── .gitignore
├── README.md
│
├── Relatorio_Wialon.xlsx
├── requirements.txt
│
├── relatorio_mes.bat
├── rodar_relatorios.bat
│
├── wialon_log.txt
├── wialon_logs.db
│
└── wialon_report_sql.py
Pré-requisitos

Antes de executar o projeto é necessário possuir:

Python 3.10 ou superior

SQL Server ou MySQL

pip

Verificar instalação do Python:

python --version
Instalação

Clone o repositório:

git clone https://github.com/LucasFarre08/API-WIALON

Acesse a pasta do projeto:

cd API-WIALON

Instale as dependências:

pip install -r requirements.txt
Configuração

Crie um arquivo .env baseado no arquivo .env.example.

Exemplo:

WIALON_TOKEN=SEU_TOKEN
WIALON_URL=https://hst-api.wialon.com/wialon/ajax.html

DB_SERVER=localhost
DB_DATABASE=telemetria
DB_USER=usuario
DB_PASSWORD=senha
Como Executar

Execute o script principal:

python wialon_report_sql.py

O sistema irá:

Autenticar na API Wialon

Executar o relatório configurado

Extrair os dados

Processar as informações

Inserir os dados no banco

Automação

O projeto inclui scripts para execução automática:

rodar_relatorios.bat
relatorio_mes.bat

Esses arquivos podem ser utilizados no Agendador de Tarefas do Windows para rodar relatórios automaticamente.

Logs

Os registros de execução são armazenados em:

wialon_log.txt
wialon_logs.db

Esses logs ajudam no diagnóstico de erros e monitoramento do sistema.

API Wialon

A integração utiliza o fluxo padrão da API:

token/login
↓
exec_report
↓
get_report_status
↓
get_report_data

Documentação oficial:

https://sdk.wialon.com/wiki/en/kit/remoteapi/apiref/apiref

Autor

Lucas Arcas Farre
GitHub:
https://github.com/LucasFarre08
