namespace KafkaProducer
{
    public record Event(Guid Id, string Name, DateTimeOffset DateTime);
}
