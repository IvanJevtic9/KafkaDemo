using KafkaConsumer;

var source = new CancellationTokenSource();
// Example - 1 - With one partition , only one consumer will receive messages

//Task.Run(() => BasicConsumerExample.Run("consumer1", "system-event", source.Token));
//Task.Run(() => BasicConsumerExample.Run("consumer2", "system-event", source.Token));

// Example - 2
Task.Run(() => BasicConsumerExample.Run("consumer1", "system-event-2", source.Token));
Task.Run(() => BasicConsumerExample.Run("consumer2", "system-event-2", source.Token));


while (true)
{
    if(Console.ReadLine() == "q")
    {
        source.Cancel();
        source.Dispose();

        break;
    }
}