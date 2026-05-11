using Confluent.Kafka;
using System.Text.Json;

namespace KafkaIntegration.Contracts
{
    public class OrderMessageSerializer : ISerializer<OrderMessage>, IDeserializer<OrderMessage>
    {
        public byte[] Serialize(OrderMessage data, SerializationContext context)
            => JsonSerializer.SerializeToUtf8Bytes(data);

        public OrderMessage Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            => JsonSerializer.Deserialize<OrderMessage>(data)!;
    }
}
