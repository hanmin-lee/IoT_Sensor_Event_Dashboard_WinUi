using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.SqlClient;

namespace IoT_Sensor_Event_Dashboard_WinUi
{
    public class KafkaConsumerService
    {
        private IConsumer<Ignore, string>? _consumer;
        private CancellationTokenSource? _cancellationTokenSource;
        private Task? _consumerTask;

        private readonly string _bootstrapServers;
        private readonly string _topic;
        private readonly string _groupId;
        private readonly SensorEventRepository _sensorEventRepository;

        private readonly List<SensorEvent> _sensorEventsBatch;
        private readonly List<IngestError> _ingestErrorsBatch;
        private readonly int _batchSize;
        private readonly int _commitIntervalMs;
        private System.Threading.Timer? _batchTimer;
        private ConsumeResult<Ignore, string>? _lastProcessedCommitResult;
        private readonly object _batchLock = new object();

        public bool IsInitialized { get; private set; } = false;
        public bool IsPaused { get; private set; } = false;
        private volatile bool _consumeEnabled = true;

        public event Action<ConsumeResult<Ignore, string>, SensorEvent>? ValidMessageProcessed;
        public event Action<ConsumeResult<Ignore, string>, IngestError>? InvalidMessageProcessed;
        public event Action<string>? ErrorOccurred;
        public event Action<string>? LogMessage;
        public event Action<int, long>? PartitionOffsetUpdated;
        public event Action<double>? MessagesPerSecondUpdated;

        public KafkaConsumerService(string bootstrapServers, string topic, string groupId,
                                    SensorEventRepository sensorEventRepository,
                                    int batchSize = 100, int commitIntervalMs = 200)
        {
            _bootstrapServers = bootstrapServers;
            _topic = topic;
            _groupId = groupId;
            _sensorEventRepository = sensorEventRepository;
            _batchSize = batchSize;
            _commitIntervalMs = commitIntervalMs;

            _sensorEventsBatch = new List<SensorEvent>();
            _ingestErrorsBatch = new List<IngestError>();
        }

        public async Task StartAsync()
        {
            if (_consumerTask != null && !_consumerTask.IsCompleted)
            {
                LogMessage?.Invoke("Consumer is already running.");
                return;
            }

            LogMessage?.Invoke("Starting Kafka Consumer...");

            try
            {
                await using (var testConnection = await _sensorEventRepository.GetOpenConnectionAsync().ConfigureAwait(false))
                {
                    LogMessage?.Invoke("Initial DB connection successful.");
                    await testConnection.CloseAsync().ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                ErrorOccurred?.Invoke($"Initial DB connection failed: {ex.Message}");
                LogMessage?.Invoke("Kafka Consumer will attempt DB during processing.");
            }

            _cancellationTokenSource = new CancellationTokenSource();
            _consumerTask = Task.Run(() => ConsumeMessages(_cancellationTokenSource.Token));

            _batchTimer = new System.Threading.Timer(async _ =>
            {
                try { await ProcessBatchesAndCommit(fromTimer: true).ConfigureAwait(false); }
                catch (Exception ex) { ErrorOccurred?.Invoke($"Batch timer error: {ex.Message}"); }
            }, null, _commitIntervalMs, _commitIntervalMs);
        }

        public void Pause()
        {
            try
            {
                if (_consumer == null) return;
                var assigned = _consumer.Assignment;
                if (assigned.Count > 0) _consumer.Pause(assigned);
                _consumeEnabled = false;
                IsPaused = true;
                LogMessage?.Invoke("Consumer paused (soft stop).");
            }
            catch (Exception ex)
            {
                ErrorOccurred?.Invoke($"Pause failed: {ex.Message}");
            }
        }

        public void Resume()
        {
            try
            {
                if (_consumer == null) return;
                var assigned = _consumer.Assignment;
                if (assigned.Count > 0) _consumer.Resume(assigned);
                _consumeEnabled = true;
                IsPaused = false;
                LogMessage?.Invoke("Consumer resumed (fast).");
            }
            catch (Exception ex)
            {
                ErrorOccurred?.Invoke($"Resume failed: {ex.Message}");
            }
        }

        public void Stop()
        {
            if (_cancellationTokenSource != null)
            {
                LogMessage?.Invoke("Stopping Kafka Consumer...");
                try { _cancellationTokenSource.Cancel(); } catch { }
                try { _consumerTask?.Wait(); } catch { }
                try { _batchTimer?.Dispose(); } catch { }
                try { _consumer?.Close(); } catch { }
                try { _consumer?.Dispose(); } catch { }

                _consumer = null;
                _consumerTask = null;
                _cancellationTokenSource = null;

                IsInitialized = false;
                IsPaused = false;
                _consumeEnabled = true;

                LogMessage?.Invoke("Kafka Consumer stopped.");
            }
        }

        private void ConsumeMessages(CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig
            {
                GroupId = _groupId,
                BootstrapServers = _bootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                MaxPollIntervalMs = 300000,
                SessionTimeoutMs = 10000,
                HeartbeatIntervalMs = 3000,
                SocketKeepaliveEnable = true,
                FetchWaitMaxMs = 250,
                MetadataMaxAgeMs = 300000
            };

            try
            {
                _consumer = new ConsumerBuilder<Ignore, string>(config)
                    .SetErrorHandler((_, e) => ErrorOccurred?.Invoke($"Kafka Error: {e.Reason}"))
                    .SetLogHandler((_, msg) => LogMessage?.Invoke($"Kafka Log ({msg.Level}): {msg.Message}"))
                    .SetPartitionsAssignedHandler((c, parts) =>
                    {
                        LogMessage?.Invoke($"Partitions assigned: {string.Join(",", parts)}");
                    })
                    .SetPartitionsRevokedHandler((c, parts) =>
                    {
                        LogMessage?.Invoke($"Partitions revoked: {string.Join(",", parts)}");
                    })
                    .Build();

                _consumer.Subscribe(_topic);
                IsInitialized = true;
                LogMessage?.Invoke($"Subscribed to topic: {_topic}");

                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = _consumer.Consume(TimeSpan.FromMilliseconds(200));
                        if (consumeResult == null || consumeResult.Message == null) continue;
                        if (!_consumeEnabled) continue;

                        _lastProcessedCommitResult = consumeResult;
                        PartitionOffsetUpdated?.Invoke(consumeResult.Partition.Value, consumeResult.Offset.Value);

                        ProcessMessage(consumeResult);

                        if (_sensorEventsBatch.Count >= _batchSize || _ingestErrorsBatch.Count >= _batchSize)
                        {
                            _batchTimer?.Change(0, _commitIntervalMs);
                        }
                    }
                    catch (ConsumeException e)
                    {
                        ErrorOccurred?.Invoke($"Consume error: {e.Error.Reason}");
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        ErrorOccurred?.Invoke($"Unhandled error during consumption: {ex.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                ErrorOccurred?.Invoke($"Kafka Consumer Service initialization error: {ex.Message}");
            }
            finally
            {
                try { _consumer?.Dispose(); } catch { }
                _consumer = null;
                IsInitialized = false;
            }
        }

        private void ProcessMessage(ConsumeResult<Ignore, string> consumeResult)
        {
            string rawJson = consumeResult.Message.Value;
            long offset = consumeResult.Offset.Value;
            int partition = consumeResult.Partition.Value;

            try
            {
                var tempSensorEvent = JsonConvert.DeserializeObject<dynamic>(rawJson);

                if (tempSensorEvent?.deviceId == null || tempSensorEvent?.ts == null || tempSensorEvent?.status == null)
                {
                    var ingestError = new IngestError
                    {
                        PartitionId = partition,
                        OffsetValue = offset,
                        ErrorType = "MissingField",
                        ErrorMessage = "Required fields (deviceId, ts, status) are missing.",
                        RawJson = rawJson,
                        IngestedAt = DateTime.UtcNow
                    };
                    lock (_batchLock) { _ingestErrorsBatch.Add(ingestError); }
                    InvalidMessageProcessed?.Invoke(consumeResult, ingestError);
                    return;
                }

                if (!DateTime.TryParse(tempSensorEvent.ts.ToString(), out DateTime eventTime))
                {
                    var ingestError = new IngestError
                    {
                        PartitionId = partition,
                        OffsetValue = offset,
                        ErrorType = "TypeError",
                        ErrorMessage = "EventTime (ts) is not a valid DateTime format.",
                        RawJson = rawJson,
                        IngestedAt = DateTime.UtcNow
                    };
                    lock (_batchLock) { _ingestErrorsBatch.Add(ingestError); }
                    InvalidMessageProcessed?.Invoke(consumeResult, ingestError);
                    return;
                }

                if (tempSensorEvent.temp != null && !decimal.TryParse(tempSensorEvent.temp.ToString(), out decimal _))
                {
                    var ingestError = new IngestError
                    {
                        PartitionId = partition,
                        OffsetValue = offset,
                        ErrorType = "TypeError",
                        ErrorMessage = "Temp is not a valid decimal format.",
                        RawJson = rawJson,
                        IngestedAt = DateTime.UtcNow
                    };
                    lock (_batchLock) { _ingestErrorsBatch.Add(ingestError); }
                    InvalidMessageProcessed?.Invoke(consumeResult, ingestError);
                    return;
                }

                if (tempSensorEvent.hum != null && !int.TryParse(tempSensorEvent.hum.ToString(), out int _))
                {
                    var ingestError = new IngestError
                    {
                        PartitionId = partition,
                        OffsetValue = offset,
                        ErrorType = "TypeError",
                        ErrorMessage = "Hum is not a valid integer format.",
                        RawJson = rawJson,
                        IngestedAt = DateTime.UtcNow
                    };
                    lock (_batchLock) { _ingestErrorsBatch.Add(ingestError); }
                    InvalidMessageProcessed?.Invoke(consumeResult, ingestError);
                    return;
                }

                var sensorEvent = new SensorEvent
                {
                    DeviceId = tempSensorEvent.deviceId!.ToString(),
                    EventTime = eventTime,
                    Temp = tempSensorEvent.temp != null ? (decimal?)tempSensorEvent.temp : null,
                    Hum = tempSensorEvent.hum != null ? (int?)tempSensorEvent.hum : null,
                    Status = tempSensorEvent.status.ToString(),
                    IngestedAt = DateTime.UtcNow
                };

                lock (_batchLock) { _sensorEventsBatch.Add(sensorEvent); }
                ValidMessageProcessed?.Invoke(consumeResult, sensorEvent);
            }
            catch (JsonSerializationException ex)
            {
                var ingestError = new IngestError
                {
                    PartitionId = partition,
                    OffsetValue = offset,
                    ErrorType = "JsonParsingError",
                    ErrorMessage = $"JSON parsing error: {ex.Message}",
                    RawJson = rawJson,
                    IngestedAt = DateTime.UtcNow
                };
                lock (_batchLock) { _ingestErrorsBatch.Add(ingestError); }
                InvalidMessageProcessed?.Invoke(consumeResult, ingestError);
            }
            catch (Exception ex)
            {
                var ingestError = new IngestError
                {
                    PartitionId = partition,
                    OffsetValue = offset,
                    ErrorType = "ProcessingError",
                    ErrorMessage = $"Error processing message: {ex.Message}",
                    RawJson = rawJson,
                    IngestedAt = DateTime.UtcNow
                };
                lock (_batchLock) { _ingestErrorsBatch.Add(ingestError); }
                InvalidMessageProcessed?.Invoke(consumeResult, ingestError);
            }
        }

        private async Task ProcessBatchesAndCommit(bool fromTimer)
        {
            List<SensorEvent> currentSensorEventsBatch;
            List<IngestError> currentIngestErrorsBatch;
            ConsumeResult<Ignore, string>? commitResult = null;

            lock (_batchLock)
            {
                if (_sensorEventsBatch.Count == 0 && _ingestErrorsBatch.Count == 0) return;

                currentSensorEventsBatch = new List<SensorEvent>(_sensorEventsBatch);
                currentIngestErrorsBatch = new List<IngestError>(_ingestErrorsBatch);

                _sensorEventsBatch.Clear();
                _ingestErrorsBatch.Clear();

                if (_lastProcessedCommitResult != null)
                {
                    commitResult = _lastProcessedCommitResult;
                    _lastProcessedCommitResult = null;
                }
            }

            using (var connection = await _sensorEventRepository.GetOpenConnectionAsync())
            {
                SqlTransaction? transaction = null;
                try
                {
                    transaction = connection.BeginTransaction();

                    if (currentSensorEventsBatch.Any())
                        await _sensorEventRepository.SaveSensorEventsAsync(currentSensorEventsBatch, transaction);

                    if (currentIngestErrorsBatch.Any())
                        await _sensorEventRepository.SaveIngestErrorsAsync(currentIngestErrorsBatch, transaction);

                    transaction.Commit();
                    LogMessage?.Invoke("Database transaction committed.");

                    // ★ 커밋 성공분을 전역 저장소에 반영
                    AfterDatabaseCommitSuccessfully(currentSensorEventsBatch);
                    AfterErrorCommitSuccessfully(currentIngestErrorsBatch); // ★ 추가

                    if (commitResult != null)
                    {
                        _consumer?.Commit(commitResult);
                        PartitionOffsetUpdated?.Invoke(commitResult.Partition.Value, commitResult.Offset.Value + 1);
                    }
                }
                catch (Exception ex)
                {
                    ErrorOccurred?.Invoke($"Database batch processing error: {ex.Message}");
                    try { transaction?.Rollback(); } catch { }
                }
            }
        }

        private void AfterDatabaseCommitSuccessfully(IEnumerable<SensorEvent> committedBatch)
        {
            if (committedBatch == null) return;
            foreach (var ev in committedBatch) App.Events?.Enqueue(ev);
        }

        private void AfterErrorCommitSuccessfully(IEnumerable<IngestError> committedErrors)
        {
            if (committedErrors == null) return;
            foreach (var er in committedErrors) App.Errors?.Enqueue(er);
        }

    }
}
