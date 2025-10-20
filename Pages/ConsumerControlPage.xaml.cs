using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Media;

namespace IoT_Sensor_Event_Dashboard_WinUi.Pages
{
    public sealed partial class ConsumerControlPage : Page
    {
        public TextBlock PartitionLabel => labelCurrentPartition;
        public TextBlock OffsetLabel => labelCurrentOffset;
        public TextBlock MpsLabel => labelMessagesPerSecond;
        public StackPanel RecentEventsHost => stackPanelRecentEvents;
        public FrameworkElement StatusSection => statusSection;
        public FrameworkElement RecentSection => recentSection;

        public ConsumerControlPage()
        {
            this.InitializeComponent();
        }
    }
}


