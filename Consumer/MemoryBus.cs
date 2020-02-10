using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Consumer
{
    public class MemoryBus
    {
        // range of random interval in ms to emulate message processing
        private readonly Tuple<int, int> _randomRange = new Tuple<int, int>(1000, 2000);

        private readonly Random _random = new Random();
        private const int QueueSize = 10;
        private readonly Queue<ConsumeResult<Ignore, int>> _messages = new Queue<ConsumeResult<Ignore, int>>();

        public event EventHandler QueueIsFull;
        public event EventHandler QueueIsEmpty;
        public event EventHandler<OnStoreOffsetEventArgs> StoreOffset;

        public void Publish(ConsumeResult<Ignore, int> message)
        {
            _messages.Enqueue(message);
            if (_messages.Count >= QueueSize)
            {
                OnQueueIsFull();
            }
        }

        public void StartMessagesProcessing(CancellationTokenSource cts)
        {
            Task.Run(() =>
            {
                while (!cts.IsCancellationRequested)
                {
                    var message = GetMessageToProcess();
                    if (message != null)
                    {
                        Thread.Sleep(_random.Next(_randomRange.Item1, _randomRange.Item2));
                        Console.WriteLine($"Partition: {message.Partition}. Offset: {message.Offset}. Value: '{message.Value}'. Messages to process: {_messages.Count}");

                        // store offset after message processing (default behavior - offsets are stored right after reading a message from Kafka)
                        OnStoreOffset(message);
                    }
                }
            });
        }

        protected virtual void OnQueueIsFull()
        {
            QueueIsFull?.Invoke(this, EventArgs.Empty);
        }

        protected virtual void OnQueueIsEmpty()
        {
            QueueIsEmpty?.Invoke(this, EventArgs.Empty);
        }

        protected virtual void OnStoreOffset(ConsumeResult<Ignore, int> message)
        {
            StoreOffset?.Invoke(this, new OnStoreOffsetEventArgs
            {
                ConsumeResult = message
            });
        }

        private ConsumeResult<Ignore, int> GetMessageToProcess()
        {
            _messages.TryDequeue(out var message);
            if (_messages.Count == 0)
            {
                OnQueueIsEmpty();
            }
            return message;
        }
    }

    public class OnStoreOffsetEventArgs : EventArgs
    {
        public ConsumeResult<Ignore, int> ConsumeResult { get; set; }
    }
}