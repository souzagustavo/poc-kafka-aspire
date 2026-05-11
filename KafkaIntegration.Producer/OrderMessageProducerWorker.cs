using Confluent.Kafka;
using KafkaIntegration.Contracts;

namespace KafkaIntegration.Producer
{
    public class OrderMessageProducerWorker(
    IProducer<string, OrderMessage> producer,
    ILogger<OrderMessageProducerWorker> logger) : BackgroundService
    {
        private const string Topic = "orders";
        private const int TotalMessages = 1000;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            logger.LogInformation("Iniciando disparo de {Total} mensagens...", TotalMessages);            

            for (int i = 1; i <= TotalMessages; i++)
            {                
                if (stoppingToken.IsCancellationRequested) break;

                var message = new OrderMessage(
                    Id: Guid.NewGuid(),
                    CustomerName: $"Cliente {i}",
                    Total: Random.Shared.Next(10, 11000),
                    CreatedAt: DateTime.UtcNow
                );

                try
                {
                    var result = await producer.ProduceAsync(
                        Topic,
                        new Message<string, OrderMessage> { Key = message.Id.ToString()!, Value = message },
                        stoppingToken);

                    logger.LogInformation(
                        "[{Index:D4}/{Total}] Publicado | OrderId: {OrderId} | Partição: {Partition} | Offset: {Offset}",
                        i,
                        TotalMessages,
                        message.Id,
                        result.Partition.Value,
                        result.Offset.Value);
                }
                catch (ProduceException<string, OrderMessage> ex)
                {
                    logger.LogError(ex,
                        "[{Index:D4}/{Total}] Falha ao publicar OrderId: {OrderId} — {Reason}",
                        i,
                        TotalMessages,
                        message.Id,
                        ex.Error.Reason);
                }
            }

            logger.LogInformation("Disparo concluído. {Total} mensagens enviadas.", TotalMessages);
        }
    }
}
