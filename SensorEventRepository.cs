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
                catch (SqlException)
                {
                    if (transaction == null)
                    {
                        currentTransactionToUse?.Rollback();
                    }
                    if (retry == maxRetries - 1) throw;
                    await Task.Delay(retryDelayMs * (retry + 1));
                }
                catch
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
                    if (retry == maxRetries - 1) throw;
                    await Task.Delay(retryDelayMs * (retry + 1));
                }
            }
            throw new Exception("Failed to open database connection after multiple retries.");
        }

        // ========= 여기부터 수정된 검색 메서드 =========
        public async Task<QueryResult<dynamic>> SearchEventsAsync(QueryParameters parameters)
        {
            var status = (parameters.StatusFilter ?? "ALL").Trim().ToUpperInvariant();
            var results = new List<dynamic>();
            int totalCount = 0;

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var keyword = parameters.Keyword?.Trim();
            bool hasKeyword = !string.IsNullOrEmpty(keyword);

            // ---------- COUNT ----------
            string countSql = status switch
            {
                "OK" => @"
SELECT COUNT(*) 
FROM dbo.SensorEvent s
WHERE s.EventTime BETWEEN @Start AND @End
  AND s.Status = 'OK'
  AND (@Keyword = '' OR s.Message LIKE @Keyword OR s.DeviceId LIKE @Keyword)",

                "ERROR" => @"
WITH U AS (
    SELECT s.Id
    FROM dbo.SensorEvent s
    WHERE s.EventTime BETWEEN @Start AND @End
      AND s.Status = 'ERROR'
      AND (@Keyword = '' OR s.Message LIKE @Keyword OR s.DeviceId LIKE @Keyword)
    UNION ALL
    SELECT ie.Id
    FROM dbo.IngestError ie
    WHERE ie.IngestedAt BETWEEN @Start AND @End
      AND (@Keyword = '' 
           OR ie.ErrorType LIKE @Keyword 
           OR ie.ErrorMessage LIKE @Keyword
           OR ie.RawJson LIKE @Keyword
                OR (ISJSON(ie.RawJson)=1 AND JSON_VALUE(ie.RawJson,'$.deviceId') LIKE @Keyword))
)
SELECT COUNT(*) FROM U;",

                _ => @"
WITH U AS (
    SELECT s.Id
    FROM dbo.SensorEvent s
    WHERE s.EventTime BETWEEN @Start AND @End
      AND (@Keyword = '' OR s.Message LIKE @Keyword OR s.DeviceId LIKE @Keyword OR s.Status LIKE @Keyword)
    UNION ALL
    SELECT ie.Id
    FROM dbo.IngestError ie
    WHERE ie.IngestedAt BETWEEN @Start AND @End
      AND (@Keyword = '' 
           OR ie.ErrorType LIKE @Keyword 
           OR ie.ErrorMessage LIKE @Keyword
           OR ie.RawJson LIKE @Keyword
           OR (ISJSON(ie.RawJson)=1 AND JSON_VALUE(ie.RawJson,'$.deviceId') LIKE @Keyword))
)
SELECT COUNT(*) FROM U;"
            };

            using (var countCmd = new SqlCommand(countSql, conn))
            {
                countCmd.Parameters.AddWithValue("@Start", parameters.StartTime);
                countCmd.Parameters.AddWithValue("@End", parameters.EndTime);
                countCmd.Parameters.AddWithValue("@Keyword", hasKeyword ? $"%{keyword}%" : "");
                totalCount = Convert.ToInt32(await countCmd.ExecuteScalarAsync());
            }

            // ---------- PAGE DATA ----------
            // ERROR/ALL에서 IngestError는 deviceId를 RawJson에서 추출, EventTime은 IngestedAt로 매핑
            string dataSql = status switch
            {
                "OK" => @"
SELECT
    'Event' AS RecordType,
    s.Id,
    s.DeviceId,
    s.EventTime,
    s.Temp,
    s.Hum,
    s.Status,
    s.IngestedAt,
    CAST(NULL AS nvarchar(100)) AS ErrorType,
    CAST(NULL AS nvarchar(max)) AS ErrorMessage,
    CAST(NULL AS nvarchar(max)) AS RawJson,
    s.EventTime AS SortTime
FROM dbo.SensorEvent s
WHERE s.EventTime BETWEEN @Start AND @End
  AND s.Status = 'OK'
  AND (@Keyword = '' OR s.Message LIKE @Keyword OR s.DeviceId LIKE @Keyword)
ORDER BY SortTime DESC
OFFSET @Offset ROWS FETCH NEXT @PageSize ROWS ONLY;",

                "ERROR" => @"
SELECT * FROM (
    SELECT
        'Event' AS RecordType,
        s.Id,
        s.DeviceId,
        s.EventTime,
        s.Temp,
        s.Hum,
        s.Status,
        s.IngestedAt,
        CAST(NULL AS nvarchar(100)) AS ErrorType,
        CAST(NULL AS nvarchar(max)) AS ErrorMessage,
        CAST(NULL AS nvarchar(max)) AS RawJson,
        s.EventTime AS SortTime
    FROM dbo.SensorEvent s
    WHERE s.EventTime BETWEEN @Start AND @End
      AND s.Status = 'ERROR'
      AND (@Keyword = '' OR s.Message LIKE @Keyword OR s.DeviceId LIKE @Keyword)

    UNION ALL

    SELECT
        'Error' AS RecordType,
        ie.Id,
        ISNULL(
  CASE WHEN ISJSON(ie.RawJson)=1 
       THEN JSON_VALUE(ie.RawJson,'$.deviceId') 
  END, 
  '-') AS DeviceId,
        ie.IngestedAt AS EventTime,                                    -- ★ 시간 매핑
        CAST(NULL AS decimal(18,2)) AS Temp,
        CAST(NULL AS int) AS Hum,
        'ERROR' AS Status,
        ie.IngestedAt,
        ie.ErrorType,
        ie.ErrorMessage,
        ie.RawJson,
        ie.IngestedAt AS SortTime
    FROM dbo.IngestError ie
    WHERE ie.IngestedAt BETWEEN @Start AND @End
      AND (@Keyword = '' 
           OR ie.ErrorType LIKE @Keyword 
           OR ie.ErrorMessage LIKE @Keyword
           OR ie.RawJson LIKE @Keyword
           OR (ISJSON(ie.RawJson)=1 AND JSON_VALUE(ie.RawJson,'$.deviceId') LIKE @Keyword))

) U
ORDER BY U.SortTime DESC
OFFSET @Offset ROWS FETCH NEXT @PageSize ROWS ONLY;",

                _ => @"
SELECT * FROM (
    SELECT
        'Event' AS RecordType,
        s.Id,
        s.DeviceId,
        s.EventTime,
        s.Temp,
        s.Hum,
        s.Status,
        s.IngestedAt,
        CAST(NULL AS nvarchar(100)) AS ErrorType,
        CAST(NULL AS nvarchar(max)) AS ErrorMessage,
        CAST(NULL AS nvarchar(max)) AS RawJson,
        s.EventTime AS SortTime
    FROM dbo.SensorEvent s
    WHERE s.EventTime BETWEEN @Start AND @End
      AND (@Keyword = '' OR s.Message LIKE @Keyword OR s.DeviceId LIKE @Keyword OR s.Status LIKE @Keyword)

    UNION ALL

    SELECT
        'Error' AS RecordType,
        ie.Id,
        ISNULL(
  CASE WHEN ISJSON(ie.RawJson)=1 
       THEN JSON_VALUE(ie.RawJson,'$.deviceId') 
  END, 
  '-') AS DeviceId,
        ie.IngestedAt AS EventTime,                                    -- ★
        CAST(NULL AS decimal(18,2)) AS Temp,
        CAST(NULL AS int) AS Hum,
        'ERROR' AS Status,
        ie.IngestedAt,
        ie.ErrorType,
        ie.ErrorMessage,
        ie.RawJson,
        ie.IngestedAt AS SortTime
    FROM dbo.IngestError ie
    WHERE ie.IngestedAt BETWEEN @Start AND @End
      AND (@Keyword = '' 
     OR ie.ErrorType LIKE @Keyword 
     OR ie.ErrorMessage LIKE @Keyword
     OR ie.RawJson LIKE @Keyword
     OR (ISJSON(ie.RawJson)=1 AND JSON_VALUE(ie.RawJson,'$.deviceId') LIKE @Keyword))
) U
ORDER BY U.SortTime DESC
OFFSET @Offset ROWS FETCH NEXT @PageSize ROWS ONLY;"
            };

            using (var dataCmd = new SqlCommand(dataSql, conn))
            {
                dataCmd.Parameters.AddWithValue("@Start", parameters.StartTime);
                dataCmd.Parameters.AddWithValue("@End", parameters.EndTime);
                dataCmd.Parameters.AddWithValue("@Keyword", hasKeyword ? $"%{keyword}%" : "");
                dataCmd.Parameters.AddWithValue("@Offset", (parameters.PageNumber - 1) * parameters.PageSize);
                dataCmd.Parameters.AddWithValue("@PageSize", parameters.PageSize);

                using var reader = await dataCmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var recordType = reader["RecordType"]?.ToString();

                    // 안전 보정: EventTime이 null이면 IngestedAt 사용
                    var eventTime = reader["EventTime"] is DBNull
                        ? (DateTime)reader["IngestedAt"]
                        : (DateTime)reader["EventTime"];

                    // 안전 보정: DeviceId가 null/빈문자면 '-'
                    var deviceIdRaw = reader["DeviceId"] is DBNull ? null : reader["DeviceId"]?.ToString();
                    var deviceId = string.IsNullOrWhiteSpace(deviceIdRaw) ? "-" : deviceIdRaw;

                    results.Add(new
                    {
                        RecordType = recordType,
                        Id = reader["Id"],
                        DeviceId = deviceId,
                        EventTime = eventTime,
                        Temp = reader["Temp"] is DBNull ? (decimal?)null : Convert.ToDecimal(reader["Temp"]),
                        Hum = reader["Hum"] is DBNull ? (int?)null : Convert.ToInt32(reader["Hum"]),
                        Status = reader["Status"]?.ToString(),
                        IngestedAt = (DateTime)reader["IngestedAt"],
                        ErrorType = reader["ErrorType"] is DBNull ? null : reader["ErrorType"]?.ToString(),
                        ErrorMessage = reader["ErrorMessage"] is DBNull ? null : reader["ErrorMessage"]?.ToString(),
                        RawJson = reader["RawJson"] is DBNull ? null : reader["RawJson"]?.ToString()
                    });
                }
            }

            var totalPages = (int)Math.Ceiling(totalCount / (double)parameters.PageSize);

            return new QueryResult<dynamic>
            {
                Items = results,
                TotalCount = totalCount,
                TotalPages = totalPages,
                CurrentPage = parameters.PageNumber,
                PageSize = parameters.PageSize
            };
        }
        // ========= 수정 끝 =========


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
