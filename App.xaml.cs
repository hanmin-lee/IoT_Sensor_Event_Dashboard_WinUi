using System;
using System.Collections.Generic;
using Microsoft.UI.Xaml;
using Microsoft.Extensions.Configuration;
using Microsoft.UI.Dispatching;
using WinRT.Interop;

namespace IoT_Sensor_Event_Dashboard_WinUi
{
    public partial class App : Application
    {
        private Window? _window;

        public static IntPtr MainHwnd { get; private set; }
        public static EventStore Events { get; private set; } = null!;
        public static ErrorStore Errors { get; private set; } = null!;   // ★ 추가

        public App()
        {
            this.InitializeComponent();

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

        protected override void OnLaunched(Microsoft.UI.Xaml.LaunchActivatedEventArgs args)
        {
            _window = new MainWindow();
            _window.Activate();

            MainHwnd = WindowNative.GetWindowHandle(_window);
            var dq = DispatcherQueue.GetForCurrentThread();

            Events = new EventStore(dq, maxItems: 2000, flushMs: 50);
            Errors = new ErrorStore(dq, maxItems: 2000, flushMs: 50);     // ★ 추가
        }
    }
}
