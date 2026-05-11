using Confluent.Kafka;

var builder = Host.CreateApplicationBuilder(args);

// Registra o consumer com o grupo "orders-consumer-group"
// "kafka" deve ser o mesmo nome definido no AppHost
builder.AddKafkaConsumer<string, string>("kafka", settings =>
{
    settings.Config.GroupId = "orders-consumer-group";
    settings.Config.AutoOffsetReset = AutoOffsetReset.Earliest;
});

// Registra o Worker
builder.Services.AddHostedService<KafkaConsumerWorker>();

builder.Build().Run();