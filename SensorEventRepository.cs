using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using System.Linq; // Added for LINQ operations

namespace IoT_Sensor_Event_Dashboard_WinUi
{
    public class SensorEventRepository
    {
        private readonly string _connectionString;
        public Action<string>? LogAction { get; set; } // New property for logging

        public SensorEventRepository(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task SaveSensorEventsAsync(List<SensorEvent> events, SqlTransaction? transaction = null)
        {
            string sql = @"INSERT INTO dbo.SensorEvent (DeviceId, EventTime, Temp, Hum, Status, IngestedAt)
                           VALUES (@DeviceId, @EventTime, @Temp, @Hum, @Status, @IngestedAt);";

            await ExecuteBatchAsync(sql, events, (cmd, ev) =>
            {
                cmd.Parameters.AddWithValue("@DeviceId", ev.DeviceId);
                cmd.Parameters.AddWithValue("@EventTime", ev.EventTime);
                cmd.Parameters.AddWithValue("@Temp", ev.Temp ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@Hum", ev.Hum ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@Status", ev.Status);
                cmd.Parameters.AddWithValue("@IngestedAt", ev.IngestedAt);
            }, transaction);
        }

        public async Task SaveIngestErrorsAsync(List<IngestError> errors, SqlTransaction? transaction = null)
        {
            string sql = @"INSERT INTO dbo.IngestError (PartitionId, OffsetValue, ErrorType, ErrorMessage, RawJson, IngestedAt)
                           VALUES (@PartitionId, @OffsetValue, @ErrorType, @ErrorMessage, @RawJson, @IngestedAt);";

            await ExecuteBatchAsync(sql, errors, (cmd, err) =>
            {
                cmd.Parameters.AddWithValue("@PartitionId", err.PartitionId);
                cmd.Parameters.AddWithValue("@OffsetValue", err.OffsetValue);
                cmd.Parameters.AddWithValue("@ErrorType", err.ErrorType);
                cmd.Parameters.AddWithValue("@ErrorMessage", err.ErrorMessage ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@RawJson", err.RawJson ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@IngestedAt", err.IngestedAt);
            }, transaction);
        }

        private async Task ExecuteBatchAsync<T>(string sql, List<T> items, Action<SqlCommand, T> addParameters, SqlTransaction? transaction)
        {
            if (items == null || items.Count == 0) return;

            const int maxRetries = 3; // 최대 3번 재시도
            const int retryDelayMs = 1000; // 1초 지연

            for (int retry = 0; retry < maxRetries; retry++)
            {
                SqlConnection? connToUse = null;
                SqlTransaction? currentTransactionToUse = transaction;

                try
                {
                    if (transaction == null)
                    {
                        connToUse = new SqlConnection(_connectionString);
                        await connToUse.OpenAsync();
                        currentTransactionToUse = connToUse.BeginTransaction();
                    }
                    else
                    {
                        connToUse = transaction.Connection;
                    }

                    foreach (var item in items)
                    {
                        using (SqlCommand cmd = new SqlCommand(sql, connToUse, currentTransactionToUse))
                        {
                            addParameters(cmd, item);
                            await cmd.ExecuteNonQueryAsync();
                        }
                    }

                    if (transaction == null)
                    {
                        currentTransactionToUse?.Commit();
                    }
                    return; // 성공했으면 루프 종료
                }
                catch (SqlException ex) // SQL 관련 예외만 재시도
                {
                    if (transaction == null)
                    {
                        currentTransactionToUse?.Rollback();
                    }
                    // 마지막 재시도였다면 예외를 다시 던짐
                    if (retry == maxRetries - 1)
                    {
                        throw; 
                    }
                    // 그렇지 않으면 잠시 기다린 후 다시 시도
                    await Task.Delay(retryDelayMs * (retry + 1)); // 지연 시간 증가
                }
                catch // 그 외 다른 예외는 즉시 다시 던짐
                {
                    if (transaction == null)
                    {
                        currentTransactionToUse?.Rollback();
                    }
                    throw;
                }
                finally
                {
                    if (transaction == null && connToUse != null)
                    {
                        connToUse.Close();
                        connToUse.Dispose();
                    }
                }
            }
        }

        public async Task<SqlConnection> GetOpenConnectionAsync()
        {
            const int maxRetries = 3;
            const int retryDelayMs = 2000;

            for (int retry = 0; retry < maxRetries; retry++)
            {
                try
                {
                    SqlConnection conn = new SqlConnection(_connectionString);
                    await conn.OpenAsync();
                    return conn;
                }
                catch (SqlException ex)
                {
                    LogAction?.Invoke($"[DB Connection Retry] Attempt {retry + 1}/{maxRetries} failed: {ex.Message}");
                    if (retry == maxRetries - 1)
                    {
                        throw; 
                    }
                    await Task.Delay(retryDelayMs * (retry + 1));
                }
            }
            throw new Exception("Failed to open database connection after multiple retries.");
        }

        public async Task<QueryResult<dynamic>> SearchEventsAsync(QueryParameters parameters)
        {
            List<dynamic> results = new List<dynamic>();
            int totalCount = 0;

            using (SqlConnection conn = new SqlConnection(_connectionString))
            {
                await conn.OpenAsync();

                // Build dynamic WHERE clause for filtering
                string sensorEventWhereClause = "";
                string ingestErrorWhereClause = "";

                List<string> sensorEventConditions = new List<string>();
                List<string> ingestErrorConditions = new List<string>();

                if (!string.IsNullOrEmpty(parameters.Keyword))
                {
                    // SensorEvent에 DeviceId와 Status 검색
                    sensorEventConditions.Add("(DeviceId LIKE @Keyword OR Status LIKE @Keyword)");
                    // IngestError에 ErrorType과 ErrorMessage 검색
                    ingestErrorConditions.Add("(ErrorType LIKE @Keyword OR ErrorMessage LIKE @Keyword)");
                }

                if (parameters.StatusFilter is not null && parameters.StatusFilter != "ALL")
                {
                    if (parameters.StatusFilter == "OK")
                    {
                        sensorEventConditions.Add("Status = 'OK'");
                        // IngestError는 OK 상태가 없으므로 IngestError 조건에는 추가하지 않음
                    }
                    else if (parameters.StatusFilter == "ERROR")
                    {
                        // SensorEvent 중 Error 상태를 찾을 필요는 없음 (명시적인 ErrorType이 없으므로)
                        ingestErrorConditions.Add("ErrorType != 'OK'"); // OK가 아닌 모든 ErrorType
                    }
                }

                if (parameters.StartTime != DateTime.MinValue)
                {
                    sensorEventConditions.Add("(EventTime >= @StartTime)");
                    ingestErrorConditions.Add("(IngestedAt >= @StartTime)");
                }

                if (parameters.EndTime != DateTime.MaxValue)
                {
                    sensorEventConditions.Add("(EventTime <= @EndTime)");
                    ingestErrorConditions.Add("(IngestedAt <= @EndTime)");
                }

                if (sensorEventConditions.Any()) 
                {
                    sensorEventWhereClause = " WHERE " + string.Join(" AND ", sensorEventConditions);
                }
                if (ingestErrorConditions.Any())
                {
                    ingestErrorWhereClause = " WHERE " + string.Join(" AND ", ingestErrorConditions);
                }

                // 1. Get total count
                string countSql = $@"SELECT COUNT(*) FROM (
                                  SELECT Id FROM dbo.SensorEvent {sensorEventWhereClause}
                                  UNION ALL
                                  SELECT Id FROM dbo.IngestError {ingestErrorWhereClause}
                                  ) AS CombinedResults";

                using (SqlCommand countCmd = new SqlCommand(countSql, conn))
                {
                    AddQueryParameters(countCmd, parameters);
                    object? result = await countCmd.ExecuteScalarAsync(); // Use object? to handle potential null or DBNull
                    totalCount = (result == null || result == DBNull.Value) ? 0 : Convert.ToInt32(result);
                }

                // 2. Get paginated data
                string dataSql = $@"SELECT
                                      'Event' as RecordType,
                                      Id,
                                      DeviceId,
                                      EventTime,
                                      Temp,
                                      Hum,
                                      Status,
                                      IngestedAt,
                                      NULL as ErrorType,
                                      NULL as ErrorMessage,
                                      NULL as RawJson
                                  FROM dbo.SensorEvent {sensorEventWhereClause}
                                  UNION ALL
                                  SELECT
                                      'Error' as RecordType,
                                      Id,
                                      NULL as DeviceId,
                                      NULL as EventTime,
                                      NULL as Temp,
                                      NULL as Hum,
                                      ErrorType as Status, -- Map ErrorType to Status for unified display
                                      IngestedAt,
                                      ErrorType,
                                      ErrorMessage,
                                      RawJson
                                  FROM dbo.IngestError {ingestErrorWhereClause}
                                  ORDER BY IngestedAt DESC
                                  OFFSET @Offset ROWS FETCH NEXT @PageSize ROWS ONLY;";

                using (SqlCommand dataCmd = new SqlCommand(dataSql, conn))
                {
                    AddQueryParameters(dataCmd, parameters);
                    dataCmd.Parameters.AddWithValue("@Offset", (parameters.PageNumber - 1) * parameters.PageSize);
                    dataCmd.Parameters.AddWithValue("@PageSize", parameters.PageSize);

                    using (var reader = await dataCmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            results.Add(new
                            {
                                RecordType = reader["RecordType"].ToString(),
                                Id = reader["Id"],
                                DeviceId = reader["DeviceId"] is DBNull ? "N/A" : reader["DeviceId"].ToString(),
                                EventTime = reader["EventTime"] is DBNull ? DateTime.MinValue : (DateTime)reader["EventTime"],
                                Temp = reader["Temp"] is DBNull ? (decimal?)null : (decimal)reader["Temp"],
                                Hum = reader["Hum"] is DBNull ? (int?)null : (int)reader["Hum"],
                                Status = reader["Status"] is DBNull ? "N/A" : reader["Status"].ToString(),
                                IngestedAt = (DateTime)reader["IngestedAt"],
                                ErrorType = reader["ErrorType"] is DBNull ? null : reader["ErrorType"].ToString(),
                                ErrorMessage = reader["ErrorMessage"] is DBNull ? null : reader["ErrorMessage"].ToString(),
                                RawJson = reader["RawJson"] is DBNull ? null : reader["RawJson"].ToString()
                            });
                        }
                    }
                }
            }

            int totalPages = (int)Math.Ceiling(totalCount / (double)parameters.PageSize);

            return new QueryResult<dynamic>
            {
                Items = results,
                TotalCount = totalCount,
                TotalPages = totalPages,
                CurrentPage = parameters.PageNumber,
                PageSize = parameters.PageSize
            };
        }

        private void AddQueryParameters(SqlCommand cmd, QueryParameters parameters)
        {
            if (!string.IsNullOrEmpty(parameters.Keyword))
            {
                cmd.Parameters.AddWithValue("@Keyword", $"%{parameters.Keyword}%");
            }
            if (parameters.StatusFilter is not null && parameters.StatusFilter != "ALL") // Added null check
            {
                cmd.Parameters.AddWithValue("@StatusFilter", parameters.StatusFilter);
            }
            if (parameters.StartTime != DateTime.MinValue)
            {
                cmd.Parameters.AddWithValue("@StartTime", parameters.StartTime);
            }
            if (parameters.EndTime != DateTime.MaxValue)
            {
                cmd.Parameters.AddWithValue("@EndTime", parameters.EndTime);
            }
        }
    }
}
