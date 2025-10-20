using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Controls.Primitives;
using Microsoft.UI.Xaml.Data;
using Microsoft.UI.Xaml.Input;
using Microsoft.UI.Xaml.Media;
using Microsoft.UI.Xaml.Navigation;
using Windows.ApplicationModel;
using Windows.ApplicationModel.Activation;

// Configuration을 위해 추가
using Microsoft.Extensions.Configuration;
using System.Threading.Tasks; // Task 사용을 위해 추가

// To learn more about WinUI, the WinUI project structure,
// and more about our project templates, see: http://aka.ms/winui-project-info.

namespace IoT_Sensor_Event_Dashboard_WinUi
{
    /// <summary>
    /// Provides application-specific behavior to supplement the default Application class.
    /// </summary>
    public partial class App : Application
    {
        private Window? _window;

        /// <summary>
        /// Initializes the singleton application object.  This is the first line of authored code
        /// executed, and as such is the logical equivalent of main() or WinMain().
        /// </summary>
        public App()
        {
            this.InitializeComponent();

            // AppSettingsManager 초기화 - WinUI 3에서는 파일 접근 제한으로 인해 기본값만 사용
            var defaultConfig = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["Profile"] = "Local",
                    ["Local:KafkaBootstrap"] = "localhost:9092",
                    ["Local:Topic"] = "sensor.events",
                    ["Local:GroupId"] = "sensor-dashboard-local",
                    ["Local:ConnectionString"] = "Server=(localdb)\\MSSQLLocalDB;Database=SensorDb;Trusted_Connection=True;"
                })
                .Build();
            AppSettingsManager.Initialize(defaultConfig);
        }

        /// <summary>
        /// Invoked when the application is launched normally by the end user.
        /// Other entry points will typically use a different method to open the app.
        /// </summary>
        /// <param name="args">Details about the launch request and process.</param>
        protected override void OnLaunched(Microsoft.UI.Xaml.LaunchActivatedEventArgs args)
        {
            _window = new MainWindow();
            _window.Activate();
        }

    }
}
