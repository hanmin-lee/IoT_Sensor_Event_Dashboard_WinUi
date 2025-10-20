using System;
using System.Collections.Generic;

namespace IoT_Sensor_Event_Dashboard_WinUi
{
    public class QueryParameters
    {
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public required string Keyword { get; set; }
        public required string StatusFilter { get; set; }
        public int PageNumber { get; set; }
        public int PageSize { get; set; }
    }

    public class QueryResult<T>
    {
        public required List<T> Items { get; set; }
        public int TotalCount { get; set; }
        public int TotalPages { get; set; }
        public int CurrentPage { get; set; }
        public int PageSize { get; set; }
    }
}
