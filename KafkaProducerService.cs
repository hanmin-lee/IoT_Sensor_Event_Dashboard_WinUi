using Confluent.Kafka;
using System;
using System.Threading.Tasks;

namespace IoT_Sensor_Event_Dashboard_WinUi
{
    public class KafkaProducerService
    {
        private readonly string _bootstrapServers;
        private readonly string _topic;

        public KafkaProducerService(string bootstrapServers, string topic)
        {
            _bootstrapServers = bootstrapServers;
            _topic = topic;
        }

        public async Task SendTestMessagesAsync()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = _bootstrapServers,
                Acks = Acks.All,
                LingerMs = 5
            };

            using var producer = new ProducerBuilder<Null, string>(config).Build();

            string[] messages =
            {
                // 정상
                @"{""deviceId"":""dev-001"",""ts"":""2025-10-13T09:00:00Z"",""temp"":27.3,""hum"":55,""status"":""OK""}",
                // ts 누락
                @"{""deviceId"":""dev-001"",""temp"":27.3,""hum"":55,""status"":""OK""}",
                // temp 타입 오류
                @"{""deviceId"":""dev-002"",""ts"":""2025-10-13T09:01:00Z"",""temp"":""hot"",""hum"":50,""status"":""OK""}"
            };

            foreach (var msg in messages)
            {
                var dr = await producer.ProduceAsync(_topic, new Message<Null, string> { Value = msg });
                Console.WriteLine($"Produced to {dr.TopicPartitionOffset}: {msg}");
            }

            producer.Flush(TimeSpan.FromSeconds(3));
        }
    }
}
