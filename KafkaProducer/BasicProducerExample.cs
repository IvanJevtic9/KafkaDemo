using Confluent.Kafka;
using Newtonsoft.Json;

namespace KafkaProducer
{
    internal class BasicProducerExample
    {
        internal static async Task Run(string topicName, CancellationToken token)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                ClientId = "system-event-producer"
            };

            // code
            var rand = new Random();
            using var producer = new ProducerBuilder<Null, string>(config).Build();
            try
            {
                while (true)
                {
                    if(token.IsCancellationRequested)
                        break;

                    int eventId = rand.Next(0, 10);
                    var message = new Event(Guid.NewGuid(), $"event-{eventId}", DateTimeOffset.Now);

                    var response = await producer.ProduceAsync(
                        topicName,
                        new Message<Null, string>
                        {
                            Value = JsonConvert.SerializeObject(message)
                        });

                    Console.WriteLine($"Message published: {message.Id}:{message.Name} Topic: {response.Topic} Partition: {response.Partition.Value} Offset: {response.Offset.Value}");

                    Thread.Sleep(3000);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                producer.Dispose();
            }
        }
    }
}
