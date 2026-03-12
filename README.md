# 🚀 API WIALON
## Integração e Extração de Relatórios de Telemetria

Projeto desenvolvido em **Python** para integração com a **API da Wialon**, permitindo a execução automática de relatórios de telemetria e armazenamento dos dados em **SQL Server ou MySQL**.

A aplicação automatiza a coleta de dados como **viagens, consumo de combustível, veículos e motoristas**, facilitando análises operacionais e geração de relatórios.

---

# 📌 Funcionalidades

- 🔐 Autenticação na API Wialon
- 📊 Execução automática de relatórios
- 📡 Extração de dados de telemetria
- 🔄 Processamento e tratamento de dados
- 💾 Armazenamento em SQL Server ou MySQL
- 📁 Exportação de relatórios
- 📝 Sistema de logs de execução
- ⚙️ Automação via scripts `.bat`

---

# 🛠 Tecnologias Utilizadas

- Python **3.10+**
- Wialon **Remote API**
- **Pandas**
- **Requests**
- **PyODBC**
- **SQL Server / MySQL**
- **OpenPyXL**

---

# 📂 Estrutura do Projeto
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

---

# ⚙️ Pré-requisitos

Antes de executar o projeto, é necessário possuir:

- Python **3.10 ou superior**
- **SQL Server** ou **MySQL**
- **pip**

Verificar se o Python está instalado:

```bash
python --version
```

📥 Instalação

Clone o repositório:

git clone https://github.com/LucasFarre08/API-WIALON

Entre na pasta do projeto:

cd API-WIALON

Instale as dependências:

pip install -r requirements.txt
⚙️ Configuração

Crie um arquivo .env baseado no .env.example.

Exemplo:

WIALON_TOKEN=SEU_TOKEN
WIALON_URL=https://hst-api.wialon.com/wialon/ajax.html

DB_SERVER=localhost
DB_DATABASE=telemetria
DB_USER=usuario
DB_PASSWORD=senha
▶️ Execução

Execute o script principal:

python wialon_report_sql.py

O sistema irá:

Autenticar na API Wialon

Executar o relatório configurado

Extrair os dados

Processar as informações

Inserir os dados no banco

🤖 Automação

O projeto inclui scripts para execução automática:

rodar_relatorios.bat
relatorio_mes.bat

Eles podem ser utilizados no Agendador de Tarefas do Windows para execução automática dos relatórios.

📝 Logs

Os registros de execução ficam armazenados em:

wialon_log.txt
wialon_logs.db

Esses arquivos ajudam no diagnóstico de erros e monitoramento do sistema.

🔗 API Utilizada

Fluxo utilizado da Wialon Remote API:

token/login
      ↓
exec_report
      ↓
get_report_status
      ↓
get_report_data

Documentação oficial:

https://sdk.wialon.com/wiki/en/kit/remoteapi/apiref/apiref

👨‍💻 Autor

Lucas Farre

GitHub:
https://github.com/LucasFarre08
