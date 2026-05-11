using Confluent.Kafka;
using KafkaIntegration.Contracts;
using KafkaIntegration.Producer;
using KafkaIntegration.ServiceDefaults;
using Microsoft.AspNetCore.Mvc;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();

builder.AddKafkaProducer<string, OrderMessage>("kafka", settings =>
{
    settings.SetValueSerializer(new OrderMessageSerializer());
});

builder.Services.AddHostedService<KafkaTopicInitializer>();
builder.Services.AddHostedService<OrderMessageProducerWorker>();

var app = builder.Build();

app.MapDefaultEndpoints();

app.MapPost("/orders", async (
    [FromBody] OrderMessageDto order,
    IProducer<string, OrderMessage> producer) =>
{
    var result = await producer.ProduceAsync(
        "orders",
        new Message<string, OrderMessage>
        {
            Key = Guid.NewGuid().ToString(),
            Value = new(Guid.NewGuid(), order.CustomerName, order.Total, DateTime.UtcNow)
        });

    return Results.Ok(new
    {
        Status = "Publicado",
        Partition = result.Partition.Value,
        Offset = result.Offset.Value
    });
});

app.Run();

public record OrderMessageDto(
    string CustomerName,
    decimal Total
);
