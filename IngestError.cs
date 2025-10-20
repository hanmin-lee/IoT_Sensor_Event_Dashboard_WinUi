using System;

namespace IoT_Sensor_Event_Dashboard_WinUi
{
    public class IngestError
    {
        public long Id { get; set; }
        public int PartitionId { get; set; }
        public long OffsetValue { get; set; }
        public required string ErrorType { get; set; }
        public string? ErrorMessage { get; set; }
        public string? RawJson { get; set; }
        public DateTime IngestedAt { get; set; }
    }
}
