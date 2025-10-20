using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml;

namespace IoT_Sensor_Event_Dashboard_WinUi.Pages
{
    public sealed partial class ConfigurationPage : Page
    {
        public TextBox KafkaBootstrap => textBoxKafkaBootstrapServers;
        public TextBox KafkaTopic => textBoxKafkaTopic;
        public TextBox KafkaGroupId => textBoxKafkaGroupId;
        public TextBox ConnectionStringBox => textBoxConnectionString;
        public Button TestConnectionButton => buttonTestConnection;

        public ConfigurationPage()
        {
            this.InitializeComponent();
        }
    }
}


