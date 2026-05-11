# KafkaIntegration

Solução .NET com .NET Aspire para integração com Apache Kafka, composta por uma API produtora de mensagens e um Worker Service consumidor.

## Projetos

| Projeto | Tipo | Responsabilidade |
|---|---|---|
| `KafkaIntegration.AppHost` | Aspire AppHost | Orquestrador — sobe e conecta todos os serviços |
| `KafkaIntegration.ServiceDefaults` | Class Library | Configurações compartilhadas de telemetria e health checks |
| `KafkaIntegration.Contracts` | Class Library | Modelos de mensagem e serializers compartilhados |
| `KafkaIntegration.Producer` | Minimal API | Endpoint HTTP e Background service que publica mensagens no tópico `orders` |
| `KafkaIntegration.Consumer` | Worker Service | Background service que consome mensagens do tópico `orders` |

---

## Pré-requisitos

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) em execução
- [Aspire Workload](https://learn.microsoft.com/dotnet/aspire/fundamentals/setup-tooling)

Instale o workload do Aspire caso ainda não tenha:

```bash
dotnet workload install aspire
```

---

## Como executar

### 1. Clone o repositório

```bash
git clone <url-do-repositorio>
cd KafkaIntegration
```

### 2. Restaure as dependências

```bash
dotnet restore
```

### 3. Suba a solução via Aspire

```bash
dotnet run --project KafkaIntegration.AppHost
```

O Aspire irá automaticamente:
- Baixar e iniciar o container do Apache Kafka (`confluentinc/confluent-local`)
- Iniciar a API Producer
- Iniciar o Consumer Worker Service
- Abrir o **Aspire Dashboard** com logs, métricas e traces de todos os serviços

> A URL do Dashboard será exibida no terminal após a inicialização, normalmente em `https://localhost:15888`.

