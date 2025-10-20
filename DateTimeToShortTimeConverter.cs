using Microsoft.UI.Xaml.Data;
using System;

namespace IoT_Sensor_Event_Dashboard_WinUi
{
    public class DateTimeToShortTimeConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, string language)
        {
            if (value is DateTime dateTime)
            {
                return dateTime.ToString("HH:mm:ss.fff");
            }
            return value;
        }

        public object ConvertBack(object value, Type targetType, object parameter, string language)
        {
            throw new NotImplementedException();
        }
    }
}
