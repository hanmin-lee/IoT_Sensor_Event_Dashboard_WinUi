using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json; // Add this using statement
using System.Text.Json; // For System.Text.Json.JsonDocument
using System.Text.Json.Serialization; // For JsonPropertyNameAttribute
using System.Collections.Generic; // Added for List
using System.Linq; // Added for LINQ operations
using System.Threading; // Explicitly use System.Threading.Timer
using Microsoft.Data.SqlClient; // Added for SqlTransaction type

namespace IoT_Sensor_Event_Dashboard_WinUi
{
    public class KafkaConsumerService
    {
        private IConsumer<Ignore, string>? _consumer; // Made nullable
        private CancellationTokenSource? _cancellationTokenSource; // Made nullable
        private Task? _consumerTask; // Made nullable

        private readonly string _bootstrapServers;
        private readonly string _topic;
        private readonly string _groupId;
        private readonly SensorEventRepository _sensorEventRepository; // Injected repository

        private readonly List<SensorEvent> _sensorEventsBatch; // Batch for valid events
        private readonly List<IngestError> _ingestErrorsBatch; // Batch for ingest errors
        private readonly int _batchSize; // Number of messages to process in a batch
        private readonly int _commitIntervalMs; // Time interval for committing offsets
        private System.Threading.Timer? _batchTimer; // Changed to System.Threading.Timer and made nullable
        private ConsumeResult<Ignore, string>? _lastProcessedCommitResult; // To track the last offset for commit, made nullable
        private readonly object _batchLock = new object(); // Dedicated lock for batch collections

        public event Action<ConsumeResult<Ignore, string>, SensorEvent>? ValidMessageProcessed; // Made nullable
        public event Action<ConsumeResult<Ignore, string>, IngestError>? InvalidMessageProcessed; // Made nullable
        public event Action<string>? ErrorOccurred; // Made nullable
        public event Action<string>? LogMessage; // Made nullable
        public event Action<int, long>? PartitionOffsetUpdated; // New event for partition and offset, made nullable
        public event Action<double>? MessagesPerSecondUpdated; // New event for messages per second, made nullable

        // Constructor updated to accept SensorEventRepository and batch settings
        public KafkaConsumerService(string bootstrapServers, string topic, string groupId, 
                                    SensorEventRepository sensorEventRepository, 
                                    int batchSize = 100, int commitIntervalMs = 200) // Changed from 1000 to 200
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

        public async void Start()
        {
            if (_consumerTask != null && !_consumerTask.IsCompleted)
            {
                LogMessage?.Invoke("Consumer is already running.");
                return;
            }

            LogMessage?.Invoke("Starting Kafka Consumer...");

            // Initial DB connection test
            LogMessage?.Invoke("Performing initial DB connection test...");
            try
            {
                using (var testConnection = await _sensorEventRepository.GetOpenConnectionAsync())
                {
                    LogMessage?.Invoke("Initial DB connection successful.");
                    testConnection.Close();
                }
            }
            catch (Exception ex)
            {
                ErrorOccurred?.Invoke($"Initial DB connection failed: {ex.Message}");
                LogMessage?.Invoke("Kafka Consumer will attempt to reconnect to DB during message processing.");
                // Don\'t return here, consumer should still try to start and process messages
            }

            _cancellationTokenSource = new CancellationTokenSource();
            _consumerTask = Task.Run(() => ConsumeMessages(_cancellationTokenSource.Token));

            // Initialize and start the batch timer
            _batchTimer = new System.Threading.Timer(async _ => await ProcessBatchesAndCommit(fromTimer: true),
                                    null, _commitIntervalMs, _commitIntervalMs);
        }

        public void Stop()
        {
            if (_cancellationTokenSource != null)
            {
                LogMessage?.Invoke("Stopping Kafka Consumer...");
                _cancellationTokenSource.Cancel();
                _consumerTask?.Wait(); // Wait for the consumer task to finish
                _batchTimer?.Dispose(); // Dispose the timer
                _consumer?.Close();
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
                EnableAutoCommit = false, // Manual commit
                MaxPollIntervalMs = 300000,
                // Increase session timeout and beat more frequently to prevent timeouts in steady state
                SessionTimeoutMs = 45000,
                HeartbeatIntervalMs = 3000,
                SocketKeepaliveEnable = true,
                FetchWaitMaxMs = 500,
                MetadataMaxAgeMs = 300000
            };

            try
            {
                using (_consumer = new ConsumerBuilder<Ignore, string>(config)
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
                    .Build())
                {
                    _consumer.Subscribe(_topic);
                    LogMessage?.Invoke($"Subscribed to topic: {_topic}");

                    while (!cancellationToken.IsCancellationRequested)
                    {
                        try
                        {
                            var consumeResult = _consumer.Consume(cancellationToken);
                            if (consumeResult != null && consumeResult.Message != null)
                            {
                                _lastProcessedCommitResult = consumeResult;
                                PartitionOffsetUpdated?.Invoke(consumeResult.Partition.Value, consumeResult.Offset.Value);
                                ProcessMessage(consumeResult);

                                if (_sensorEventsBatch.Count >= _batchSize || _ingestErrorsBatch.Count >= _batchSize)
                                {
                                    // Trigger batch processing if batch size is reached
                                    _batchTimer?.Change(0, _commitIntervalMs); // Reset timer to trigger immediately
                                }
                            }
                        }
                        catch (ConsumeException e)
                        {
                            ErrorOccurred?.Invoke($"Consume error: {e.Error.Reason}");
                        }
                        catch (OperationCanceledException)
                        {
                            // Consumer was cancelled
                            break;
                        }
                        catch (Exception ex)
                        {
                            ErrorOccurred?.Invoke($"Unhandled error during consumption: {ex.Message}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                ErrorOccurred?.Invoke($"Kafka Consumer Service initialization error: {ex.Message}");
            }
            finally
            {
                _consumer?.Dispose();
                _consumer = null;
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
                    var ingestError = new IngestError // Assign to local variable
                    {
                        PartitionId = partition,
                        OffsetValue = offset,
                        ErrorType = "MissingField",
                        ErrorMessage = "Required fields (deviceId, ts, status) are missing.",
                        RawJson = rawJson,
                        IngestedAt = DateTime.UtcNow
                    };
                    lock (_batchLock) // Synchronize access to batch list
                    {
                        _ingestErrorsBatch.Add(ingestError);
                    }
                    InvalidMessageProcessed?.Invoke(consumeResult, ingestError); // Pass local variable
                    return;
                }

                if (!DateTime.TryParse(tempSensorEvent.ts.ToString(), out DateTime eventTime))
                {
                    var ingestError = new IngestError // Assign to local variable
                    {
                        PartitionId = partition,
                        OffsetValue = offset,
                        ErrorType = "TypeError",
                        ErrorMessage = "EventTime (ts) is not a valid DateTime format.",
                        RawJson = rawJson,
                        IngestedAt = DateTime.UtcNow
                    };
                    lock (_batchLock) // Synchronize access to batch list
                    {
                        _ingestErrorsBatch.Add(ingestError);
                    }
                    InvalidMessageProcessed?.Invoke(consumeResult, ingestError); // Pass local variable
                    return;
                }

                if (tempSensorEvent.temp != null && !decimal.TryParse(tempSensorEvent.temp.ToString(), out decimal tempValue))
                {
                    var ingestError = new IngestError // Assign to local variable
                    {
                        PartitionId = partition,
                        OffsetValue = offset,
                        ErrorType = "TypeError",
                        ErrorMessage = "Temp is not a valid decimal format.",
                        RawJson = rawJson,
                        IngestedAt = DateTime.UtcNow
                    };
                    lock (_batchLock) // Synchronize access to batch list
                    {
                        _ingestErrorsBatch.Add(ingestError);
                    }
                    InvalidMessageProcessed?.Invoke(consumeResult, ingestError); // Pass local variable
                    return;
                }

                if (tempSensorEvent.hum != null && !int.TryParse(tempSensorEvent.hum.ToString(), out int humValue))
                {
                    var ingestError = new IngestError // Assign to local variable
                    {
                        PartitionId = partition,
                        OffsetValue = offset,
                        ErrorType = "TypeError",
                        ErrorMessage = "Hum is not a valid integer format.",
                        RawJson = rawJson,
                        IngestedAt = DateTime.UtcNow
                    };
                    lock (_batchLock) // Synchronize access to batch list
                    {
                        _ingestErrorsBatch.Add(ingestError);
                    }
                    InvalidMessageProcessed?.Invoke(consumeResult, ingestError); // Pass local variable
                    return;
                }

                var sensorEvent = new SensorEvent
                {
                    DeviceId = tempSensorEvent.deviceId!.ToString(), // Use null-forgiving operator
                    EventTime = eventTime,
                    Temp = tempSensorEvent.temp != null ? (decimal?)tempSensorEvent.temp : null, // Explicitly handle null
                    Hum = tempSensorEvent.hum != null ? (int?)tempSensorEvent.hum : null,       // Explicitly handle null
                    Status = tempSensorEvent.status.ToString(),
                    IngestedAt = DateTime.UtcNow
                };

                lock (_batchLock) // Synchronize access to batch list
                {
                    _sensorEventsBatch.Add(sensorEvent);
                }
                ValidMessageProcessed?.Invoke(consumeResult, sensorEvent); // Invoke event for valid message
            }
            catch (JsonSerializationException ex)
            {
                var ingestError = new IngestError // Assign to local variable
                {
                    PartitionId = partition,
                    OffsetValue = offset,
                    ErrorType = "JsonParsingError",
                    ErrorMessage = $"JSON parsing error: {ex.Message}",
                    RawJson = rawJson,
                    IngestedAt = DateTime.UtcNow
                };
                lock (_batchLock) // Synchronize access to batch list
                {
                    _ingestErrorsBatch.Add(ingestError);
                }
                InvalidMessageProcessed?.Invoke(consumeResult, ingestError); // Pass local variable
            }
            catch (Exception ex)
            {
                var ingestError = new IngestError // Assign to local variable
                {
                    PartitionId = partition,
                    OffsetValue = offset,
                    ErrorType = "ProcessingError",
                    ErrorMessage = $"Error processing message: {ex.Message}",
                    RawJson = rawJson,
                    IngestedAt = DateTime.UtcNow
                };
                lock (_batchLock) // Synchronize access to batch list
                {
                    _ingestErrorsBatch.Add(ingestError);
                }
                InvalidMessageProcessed?.Invoke(consumeResult, ingestError); // Pass local variable
            }
        }

        // New method to process batches and commit offsets
        private async Task ProcessBatchesAndCommit(bool fromTimer)
        {
            List<SensorEvent> currentSensorEventsBatch;
            List<IngestError> currentIngestErrorsBatch;
            ConsumeResult<Ignore, string>? commitResult = null; // Local variable for the commit result

            lock (_batchLock)
            {
                // Only process if there are items
                if (_sensorEventsBatch.Count == 0 && _ingestErrorsBatch.Count == 0)
                {
                    return;
                }

                // Create local copies of the batches
                currentSensorEventsBatch = new List<SensorEvent>(_sensorEventsBatch);
                currentIngestErrorsBatch = new List<IngestError>(_ingestErrorsBatch);
                
                // Clear the original batches immediately under lock
                _sensorEventsBatch.Clear();
                _ingestErrorsBatch.Clear();

                // Take the last processed commit result
                if (_lastProcessedCommitResult != null)
                {
                    commitResult = _lastProcessedCommitResult;
                    _lastProcessedCommitResult = null; // Clear it after taking it
                }
            }

            LogMessage?.Invoke($"Processing batch: SensorEvents={currentSensorEventsBatch.Count}, IngestErrors={currentIngestErrorsBatch.Count}");
            LogMessage?.Invoke("Attempting to get DB connection and begin transaction..."); // 추가된 로그

            using (var connection = await _sensorEventRepository.GetOpenConnectionAsync())
            {
                SqlTransaction? transaction = null;
                try
                {
                    transaction = connection.BeginTransaction();
                    LogMessage?.Invoke("DB connection opened and transaction begun."); // 추가된 로그

                    if (currentSensorEventsBatch.Any())
                    {
                        LogMessage?.Invoke($"Saving {currentSensorEventsBatch.Count} SensorEvents..."); // 추가된 로그
                        await _sensorEventRepository.SaveSensorEventsAsync(currentSensorEventsBatch, transaction);
                    }

                    if (currentIngestErrorsBatch.Any())
                    {
                        LogMessage?.Invoke($"Saving {currentIngestErrorsBatch.Count} IngestErrors..."); // 추가된 로그
                        await _sensorEventRepository.SaveIngestErrorsAsync(currentIngestErrorsBatch, transaction);
                    }

                    transaction.Commit();
                    LogMessage?.Invoke("Database transaction committed.");

                    // Commit Kafka offset only after successful DB transaction
                    if (commitResult != null)
                    {
                        _consumer?.Commit(commitResult);
                        LogMessage?.Invoke($"Kafka offset committed for Partition {commitResult.Partition.Value}, Offset {commitResult.Offset.Value}");
                        PartitionOffsetUpdated?.Invoke(commitResult.Partition.Value, commitResult.Offset.Value + 1); // Update with next expected offset
                    }
                }
                catch (Exception ex)
                {
                    ErrorOccurred?.Invoke($"Database batch processing error: {ex.Message}");
                    LogMessage?.Invoke("Database transaction rolled back. Kafka offset not committed.");
                }
            }
        }
    }
}
