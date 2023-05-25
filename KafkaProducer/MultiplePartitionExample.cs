using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Newtonsoft.Json;

namespace KafkaProducer
{
    internal class MultiplePartitionExample
    {
        internal static async Task Run(string topicName, CancellationToken token)
        {
            // setup a new topic with multiple partition

            var adminConfig = new AdminClientConfig
            {
                BootstrapServers = "localhost:9092",
            };

            using var adminClient = new AdminClientBuilder(adminConfig).Build();
            try
            {
                await adminClient.CreateTopicsAsync(new TopicSpecification[] {
                    new TopicSpecification { Name = topicName, ReplicationFactor = 1, NumPartitions = 2 } });
            }
            catch (CreateTopicsException e)
            {
                Console.WriteLine($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
            }
            finally
            {
                adminClient.Dispose();
            }

            // code
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                ClientId = "system-event-producer"
            };

            var rand = new Random();
            using var producer = new ProducerBuilder<Null, string>(config).Build();
            try
            {
                while (true)
                {
                    if (token.IsCancellationRequested)
                        break;

                    int eventId = rand.Next(0, 10);
                    var message = new Event(Guid.NewGuid(), $"event-{eventId}", DateTimeOffset.Now);

                    int partition = rand.Next(0, 3) == 0 ? 0 : 1;

                    var response = await producer.ProduceAsync(
                        new TopicPartition(topicName, partition),
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
