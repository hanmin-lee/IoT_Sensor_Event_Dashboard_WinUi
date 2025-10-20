using Microsoft.Extensions.Configuration;
using System;

namespace IoT_Sensor_Event_Dashboard_WinUi
{
    public static class AppSettingsManager
    {
        private static IConfiguration? _configuration;
        private const string ProfileKey = "Profile";

        public static void Initialize(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public static string? GetSetting(string key)
        {
            if (_configuration == null)
            {
                throw new InvalidOperationException("AppSettingsManager has not been initialized. Call Initialize() first.");
            }

            string? profile = _configuration[ProfileKey];
            if (string.IsNullOrEmpty(profile))
            {
                profile = "Local"; // Default profile if not set
            }

            // Profile specific key
            string specificKey = $"{profile}:{key}";
            string? value = _configuration[specificKey];

            if (string.IsNullOrEmpty(value))
            {
                // Fallback to non-profile specific key if profile specific is not found
                value = _configuration[key];
            }
            
            // Debug logging
            System.Diagnostics.Debug.WriteLine($"GetSetting: key={key}, profile={profile}, specificKey={specificKey}, value={value}");
            
            return value;
        }

        public static string? KafkaBootstrapServers => GetSetting("KafkaBootstrap");
        public static string? KafkaTopic => GetSetting("Topic");
        public static string? KafkaGroupId => GetSetting("GroupId");
        public static string? ConnectionString => GetSetting("ConnectionString");
    }
}
