using System;

namespace IoT_Sensor_Event_Dashboard_WinUi
{
    public class SensorEvent
    {
        public long Id { get; set; }
        public required string DeviceId { get; set; }
        public DateTime EventTime { get; set; }
        public decimal? Temp { get; set; }
        public int? Hum { get; set; }
        public required string Status { get; set; }
        public DateTime IngestedAt { get; set; }
        public string Message { get; set; } = string.Empty;
    }
}
