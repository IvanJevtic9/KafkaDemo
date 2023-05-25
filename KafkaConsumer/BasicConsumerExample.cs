using Confluent.Kafka;
using Newtonsoft.Json;
using System.Text;

namespace KafkaConsumer
{
    internal class BasicConsumerExample
    {
        internal static Task Run(string consumerName, string topicName, CancellationToken cancellationToken)
        {
            // config
            var config = new ConsumerConfig
            {
                GroupId = "system-event-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            // Basic consumer
            using var consumer = new ConsumerBuilder<Null, string>(config).Build();
            consumer.Subscribe(topicName);

            try
            {
                while (true)
                {
                    if (cancellationToken.IsCancellationRequested)
                        break;

                    var response = consumer.Consume(cancellationToken);

                    if (response.Message != null)
                    {
                        var eventObj = JsonConvert.DeserializeObject<Event>(response.Message.Value);

                        var strBu = new StringBuilder();

                        strBu.AppendLine($"Consumer: {consumerName}");
                        strBu.AppendLine("ID: " + eventObj.Id);
                        strBu.AppendLine("Name: " + eventObj.Name);
                        strBu.AppendLine("Time: " + eventObj.DateTime.ToString());
                        strBu.AppendLine("Consumed from: " + response.Partition.Value);

                        Console.Write(strBu.ToString());
                    }
                }
            }
            catch (ProduceException<Null, string> exc)
            {
                Console.WriteLine(exc.Message);
            }
            finally
            {
                consumer.Dispose();
            }

            return Task.CompletedTask;
        }
    }
}
