namespace KafkaIntegration.Contracts;

public record OrderMessage(
    Guid? Id,
    string CustomerName,
    decimal Total,
    DateTime CreatedAt
);