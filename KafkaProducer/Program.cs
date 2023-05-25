using KafkaProducer;

var source = new CancellationTokenSource();

// Basic Producer - Example 1

//Task.Run(async () => await BasicProducerExample.Run("system-event", source.Token));

//Producer with multiple Partition

Task.Run(async () => await MultiplePartitionExample.Run("system-event-2", source.Token));

while (true)
{
    if(Console.ReadLine() == "q")
    {
        source.Cancel();
        source.Dispose();

        break;
    }
}