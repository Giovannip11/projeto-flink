# Projeto de Detecção de Tráfego de Rede com Apache Flink & Confluent Cloud

Este projeto realiza a ingestão e o processamento em tempo real de fluxos de dados de tráfego de rede (utilizando o dataset IDS 2017) para simulação de monitoramento de segurança e detecção de DDoS.

## 🛠️ Arquitetura do Projeto

1. **Ingestor (Python)**: Lê o dataset local em CSV e envia os dados (JSON) para um tópico no Confluent Cloud (Apache Kafka).
2. **Mensageria (Confluent Cloud)**: Armazena o fluxo de eventos de forma segura e escalável na nuvem.
3. **Processamento (Apache Flink no Docker)**: Consome os dados do Confluent em tempo real, realizando agregações e análises contínuas.

---

## 🚀 Como Rodar o Projeto

### 1. Pré-requisitos
Certifique-se de ter instalado em sua máquina:
* **Python 3.11** (versão recomendada e estável para as dependências do Flink)
* **Docker** e **Docker Compose**
* O dataset do IDS 2017 salvo na pasta `./data/` com o nome `Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv`.

---

### 2. Configurando as Variáveis de Ambiente (`.env`)
Na raiz do projeto, crie um arquivo chamado `.env` e adicione as suas credenciais da Confluent Cloud obtidas no painel do cluster Kafka:

```env
CONFLUENT_BOOTSTRAP_SERVER=seu_endereco_do_bootstrap_server:9092
CONFLUENT_API_KEY=sua_api_key_aqui
CONFLUENT_API_SECRET=seu_api_secret_aqui
