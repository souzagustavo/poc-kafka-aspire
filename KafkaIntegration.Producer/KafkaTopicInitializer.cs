using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace KafkaIntegration.Producer
{
    public class KafkaTopicInitializer(
        IConfiguration config,
        ILogger<KafkaTopicInitializer> logger) : IHostedService
    {
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            // Pega a connection string injetada pelo Aspire
            var bootstrapServers = config.GetConnectionString("kafka");

            using var adminClient = new AdminClientBuilder(
                new AdminClientConfig { BootstrapServers = bootstrapServers })
                .Build();

            try
            {
                await adminClient.CreateTopicsAsync(
                [
                    new TopicSpecification
                {
                    Name              = "orders",
                    NumPartitions     = 1,
                    ReplicationFactor = 1
                }
                ]);

                logger.LogInformation("Tópico 'orders' criado com sucesso.");
            }
            catch (CreateTopicsException ex)
                when (ex.Results[0].Error.Code == ErrorCode.TopicAlreadyExists)
            {
                // Tópico já existe — sem problema
                logger.LogInformation("Tópico 'orders' já existe.");
            }
        }

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
