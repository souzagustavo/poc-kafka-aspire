using Confluent.Kafka;

public class KafkaConsumerWorker(
    IConsumer<string, string> consumer,
    ILogger<KafkaConsumerWorker> logger) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") == "Development")
        {
            logger.LogInformation("Aguardando 2 segundos para o Kafka iniciar e criar o tópico...");
            Task.Delay(10000, stoppingToken).Wait(stoppingToken);            
        }

        consumer.Subscribe("orders");

        return Task.Run(() =>
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {                    
                    var result = consumer.Consume(stoppingToken);

                    logger.LogInformation(
                        "Mensagem recebida | Tópico: {Topic} | Partição: {Partition} | Offset: {Offset} | Valor: {Value}",
                        result.Topic,
                        result.Partition.Value,
                        result.Offset.Value,
                        result.Message.Value);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Erro ao consumir mensagem do Kafka");
                }
            }
            
            consumer.Close();

        }, stoppingToken);
    }
}