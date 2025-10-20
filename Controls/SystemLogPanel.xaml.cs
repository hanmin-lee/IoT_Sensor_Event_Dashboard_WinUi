using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;

namespace IoT_Sensor_Event_Dashboard_WinUi.Controls
{
    public sealed partial class SystemLogPanel : UserControl
    {
        public TextBox LogTextBox => textBoxLog;
        public Button ClearButton => btnLogClear;

        public SystemLogPanel()
        {
            this.InitializeComponent();
        }
    }
}



