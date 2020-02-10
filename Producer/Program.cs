using System;
using System.Threading;
using Confluent.Kafka;

namespace Producer
{
    class Program
    {
        // range of random interval in ms to wait between messages producing
        private static Tuple<int, int> _randomRange = new Tuple<int, int>(1500, 2500);

        static void Main(string[] args)
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            // If serializers are not specified, default serializers from
            // `Confluent.Kafka.Serializers` will be automatically used where
            // available. Note: by default strings are encoded as UTF8.

            void DeliveryHandler(DeliveryReport<Null, int> r)
            {
                Console.WriteLine(!r.Error.IsError
                    ? $"Delivered message {r.Message.Value} to {r.TopicPartitionOffset}"
                    : $"Delivery Error: {r.Error.Reason}");
            }

            using (var p = new ProducerBuilder<Null, int>(config).Build())
            {
                var count = 0;
                var random = new Random();

                while (true)
                {
                    p.Produce("test-topic", new Message<Null, int> { Value = count++ }, DeliveryHandler);
                    Thread.Sleep(random.Next(_randomRange.Item1, _randomRange.Item2));
                }
            }
        }
    }
}
