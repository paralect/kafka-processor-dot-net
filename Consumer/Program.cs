using System;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;

namespace Consumer
{
    class Program
    {
        private static bool _isQueueFull;
        private static IConsumer<Ignore, int> _consumer;
        private static readonly MemoryBus Bus = new MemoryBus();

        static void Main(string[] args)
        {
            Bus.QueueIsFull += Bus_QueueIsFull;
            Bus.QueueIsEmpty += Bus_QueueIsEmpty;
            Bus.StoreOffset += Bus_StoreOffset;

            var conf = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "localhost:9092",
                // Note: The AutoOffsetReset property determines the start offset in the event
                // there are not yet any committed offsets for the consumer group for the
                // topic/partitions of interest. By default, offsets are committed
                // automatically, so in this example, consumption will only start from the
                // earliest message in the topic 'my-topic' the first time you run the program.
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoOffsetStore = false,
                Debug = "consumer"
            };

            using (
                var c =
                    new ConsumerBuilder<Ignore, int>(conf)
                        .SetPartitionsAssignedHandler(PartitionsAssignedHandler)
                        .SetPartitionsRevokedHandler(PartitionsRevokedHandler)
                        .SetLogHandler(LogHandler)
                    .Build()
            )
            {
                _consumer = c;
                c.Subscribe("test-topic");

                var cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                Bus.StartMessagesProcessing(cts);

                try
                {
                    while (!cts.IsCancellationRequested)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            Bus.Publish(cr);
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                finally
                {
                    c.Close();
                }
            }

            Console.ReadLine();
        }

        private static void Bus_QueueIsFull(object sender, EventArgs e)
        {
            if (!_isQueueFull)
            {
                Console.WriteLine("Queue is full");
            }

            _isQueueFull = true;
        }

        private static void Bus_QueueIsEmpty(object sender, EventArgs e)
        {
            if (_isQueueFull)
            {
                Console.WriteLine("Queue is empty");
            }

            _isQueueFull = false;
        }

        private static void Bus_StoreOffset(object sender, OnStoreOffsetEventArgs e)
        {
            try
            {
                _consumer?.StoreOffset(e.ConsumeResult);
            }
            catch (ObjectDisposedException) // unavoidable exception on the last iteration of message processing
            {
                Console.WriteLine("Consumer is closed");
            }
        }

        private static void LogHandler(IConsumer<Ignore, int> consumer, LogMessage message)
        {
            Console.WriteLine(message.Message);
        }

        private static void PartitionsAssignedHandler(IConsumer<Ignore, int> consumer, IEnumerable<TopicPartition> partitions)
        {
            Console.WriteLine("PARTITIONS ASSIGNED");
        }

        private static void PartitionsRevokedHandler(IConsumer<Ignore, int> consumer, IEnumerable<TopicPartitionOffset> offsets)
        {
            Console.WriteLine("PARTITIONS REVOKED");
        }
    }
}
