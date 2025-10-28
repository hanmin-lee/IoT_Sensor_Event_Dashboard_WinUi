using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Media;
using Microsoft.UI.Xaml.Navigation;
using Microsoft.UI.Xaml.Controls.Primitives;
using IoT_Sensor_Event_Dashboard_WinUi.Pages;
using IoT_Sensor_Event_Dashboard_WinUi.Controls;
using System.Linq;

namespace IoT_Sensor_Event_Dashboard_WinUi
{
    public enum TestScenario
    {
        Mixed,
        ValidOnly,
        MissingTs,
        TempAsString
    }

    public sealed partial class MainWindow : Window
    {
        private KafkaConsumerService? _kafkaConsumerService;
        private SensorEventRepository? _sensorEventRepository;
        private DispatcherTimer? _uiUpdateTimer;

        private long _messagesProcessedCount = 0;
        private DateTime _lastUiUpdateTime = DateTime.UtcNow;
        private const int MaxLogLines = 5000;

        private int _currentPage = 1;
        private int _pageSize = 20;
        private int _totalQueryPages = 1;

        private readonly Dictionary<string, Button> _navRailButtons = new();
        private readonly SolidColorBrush _navTransparentBrush = new SolidColorBrush(Microsoft.UI.Colors.Transparent);
        private string _activeNavTag = string.Empty;

        public MainWindow()
        {
            InitializeComponent();
            Closed += MainWindow_Closed;
            InitializeApplicationComponents();

            SetupListViewColumns();
            SetupQueryResultListViewColumns();
            InitializeQueryTab();

            _navRailButtons["consumer"] = NavButtonConsumer;
            _navRailButtons["query"] = NavButtonQuery;
            _navRailButtons["config"] = NavButtonConfig;

            try { systemLog.ClearButton.Click += btnLogClear_Click; } catch { }
            try { NavigateTo("consumer"); } catch { UpdateRailSelection("consumer"); }
        }

        private void MainWindow_Closed(object sender, WindowEventArgs args)
        {
            _kafkaConsumerService?.Stop();
        }

        private void InitializeApplicationComponents()
        {
            string bootstrapServers = AppSettingsManager.KafkaBootstrapServers ?? "localhost:9092";
            string topic = AppSettingsManager.KafkaTopic ?? "sensor.events";
            string groupId = AppSettingsManager.KafkaGroupId ?? "sensor-dashboard-local";
            string connectionString = AppSettingsManager.ConnectionString ?? "Server=(localdb)\\MSSQLLocalDB;Database=SensorDb;Trusted_Connection=True;";

            _sensorEventRepository = new SensorEventRepository(connectionString);
            _sensorEventRepository.LogAction = LogToRichTextBox;

            _kafkaConsumerService = new KafkaConsumerService(bootstrapServers, topic, groupId, _sensorEventRepository);
            _kafkaConsumerService.LogMessage += LogToRichTextBox;
            _kafkaConsumerService.ErrorOccurred += LogErrorToRichTextBox;
            _kafkaConsumerService.PartitionOffsetUpdated += UpdatePartitionOffsetLabels;

            _uiUpdateTimer = new DispatcherTimer { Interval = TimeSpan.FromSeconds(1) };
            _uiUpdateTimer.Tick += UiUpdateTimer_Tick;
            _uiUpdateTimer.Start();

            LogToRichTextBox("Application initialized.");
            LogToRichTextBox($"Loaded Kafka Bootstrap Servers: {bootstrapServers}");
            LogToRichTextBox($"Loaded Kafka Topic: {topic}");
            LogToRichTextBox($"Loaded Kafka Group ID: {groupId}");
            LogToRichTextBox($"Loaded DB Connection String (partial): {connectionString.Substring(0, Math.Min(50, connectionString.Length))}...");
        }

        private void EnsureMaxLogLines()
        {
            string text = systemLog.LogTextBox.Text ?? string.Empty;
            int newlineSeen = 0;
            for (int i = text.Length - 1; i >= 0; i--)
            {
                if (text[i] == '\n')
                {
                    newlineSeen++;
                    if (newlineSeen > MaxLogLines)
                    {
                        systemLog.LogTextBox.Text = text[(i + 1)..];
                        break;
                    }
                }
            }
        }

        private void AppendLog(string line)
        {
            try
            {
                systemLog.LogTextBox.Text += line + "\n";
                EnsureMaxLogLines();

                systemLog.LogTextBox.SelectionStart = systemLog.LogTextBox.Text.Length;
                systemLog.LogTextBox.SelectionLength = 0;

                if (VisualTreeHelper.GetChild(systemLog.LogTextBox, 0) is ScrollViewer viewer)
                    viewer.ChangeView(null, viewer.ExtentHeight, null, true);
            }
            catch { }
        }

        private void LogToRichTextBox(string message)
        {
            DispatcherQueue?.TryEnqueue(() => AppendLog($"[{DateTime.Now:HH:mm:ss}] {message}"));
        }

        private void LogErrorToRichTextBox(string errorMessage)
        {
            DispatcherQueue?.TryEnqueue(() => AppendLog($"[{DateTime.Now:HH:mm:ss}] ERROR: {errorMessage}"));
        }

        private void SetupListViewColumns() { }
        private void SetupQueryResultListViewColumns() { }
        private void InitializeQueryTab() { }

        // === 전역 저장소(정상+에러) → 화면 재구성 ===
        // 들어온(커밋된) 순서 그대로: IngestedAt 기준 오름차순, 오래된→최신
        private void RebuildRecentFromStore(ConsumerControlPage page)
        {
            try
            {
                if (page == null) return;

                var host = page.RecentEventsHost as Panel
                           ?? page.FindName("RecentEventsHost") as Panel;
                if (host == null)
                {
                    LogErrorToRichTextBox("RebuildRecentFromStore: RecentEventsHost not found.");
                    return;
                }

                var evs = App.Events?.TakeLast(400) ?? Array.Empty<SensorEvent>();
                var ers = App.Errors?.TakeLast(400) ?? Array.Empty<IngestError>();

                // OK/ERROR를 하나의 타임라인으로 합쳐서 IngestedAt(도착 시각) 기준 오름차순 정렬
                var timeline = evs.Select(e => new
                {
                    At = (e.IngestedAt == default ? e.EventTime : e.IngestedAt),
                    IsError = false,
                    Ev = e,
                    Er = (IngestError?)null
                })
                                .Concat(
                                    ers.Select(er => new
                                    {
                                        At = (er.IngestedAt == default ? DateTime.UtcNow : er.IngestedAt),
                                        IsError = true,
                                        Ev = (SensorEvent?)null,
                                        Er = er
                                    })
                                )
                                .OrderBy(x => x.At)  // ← 들어온 순서(오래된→최신)
                                .Take(200)
                                .ToList();

                host.Children.Clear();

                foreach (var x in timeline)
                {
                    if (!x.IsError && x.Ev != null)
                    {
                        var ev = x.Ev;
                        var row = new StackPanel { Orientation = Orientation.Horizontal, Spacing = 12, Tag = "OK" };
                        row.Children.Add(new TextBlock { Text = x.At.ToLocalTime().ToString("HH:mm:ss"), Width = 70 });
                        row.Children.Add(new TextBlock { Text = ev.DeviceId ?? "-", Width = 110 });
                        row.Children.Add(new TextBlock { Text = ev.Status ?? "-", Width = 70 });
                        row.Children.Add(new TextBlock { Text = ev.Message ?? string.Empty });
                        host.Children.Add(row);  // ← 아래로 쌓이게 Add
                    }
                    else if (x.IsError && x.Er != null)
                    {
                        var er = x.Er;
                        var row = new StackPanel { Orientation = Orientation.Horizontal, Spacing = 12, Tag = "ERROR" };
                        row.Children.Add(new TextBlock { Text = x.At.ToLocalTime().ToString("HH:mm:ss"), Width = 70 });
                        row.Children.Add(new TextBlock { Text = "-", Width = 110 });
                        row.Children.Add(new TextBlock { Text = "ERROR", Width = 70, Foreground = new SolidColorBrush(Microsoft.UI.Colors.IndianRed) });
                        row.Children.Add(new TextBlock { Text = $"{er.ErrorType}: {er.ErrorMessage}", TextWrapping = TextWrapping.WrapWholeWords });
                        host.Children.Add(row);  // ← 아래로 쌓이게 Add
                    }
                }
            }
            catch (Exception ex)
            {
                LogErrorToRichTextBox($"RebuildRecentFromStore error: {ex.Message}");
            }
        }


        // === 수신 시 화면 렌더링 + 전역 저장소 누적 (정상) ===
        private void HandleValidMessageProcessed(ConsumeResult<Ignore, string> consumeResult, SensorEvent sensorEvent)
        {
            Interlocked.Increment(ref _messagesProcessedCount);
            LogToRichTextBox($"Valid event: {sensorEvent.DeviceId} {sensorEvent.EventTime:HH:mm:ss} {sensorEvent.Status}");


            DispatcherQueue?.TryEnqueue(() =>
            {
                if (contentFrame.Content is ConsumerControlPage page)
                {
                    var row = new StackPanel { Orientation = Orientation.Horizontal, Spacing = 12 };
                    row.Children.Add(new TextBlock { Text = sensorEvent.EventTime.ToLocalTime().ToString("HH:mm:ss"), Width = 70 });
                    row.Children.Add(new TextBlock { Text = sensorEvent.DeviceId, Width = 110 });
                    row.Children.Add(new TextBlock { Text = sensorEvent.Status, Width = 70 });
                    row.Children.Add(new TextBlock { Text = sensorEvent.Message ?? string.Empty });

                    page.RecentEventsHost.Children.Add(row);

                    const int maxRows = 200;
                    while (page.RecentEventsHost.Children.Count > maxRows)
                        page.RecentEventsHost.Children.RemoveAt(page.RecentEventsHost.Children.Count - 1);
                }
            });
        }


        // === 수신 시 화면 렌더링 + 전역 저장소 누적 (에러) ===
        private void HandleInvalidMessageProcessed(ConsumeResult<Ignore, string> consumeResult, IngestError ingestError)
        {
            Interlocked.Increment(ref _messagesProcessedCount);
            LogErrorToRichTextBox($"Invalid message at {ingestError.OffsetValue}: {ingestError.ErrorType}");


            DispatcherQueue?.TryEnqueue(() =>
            {
                if (contentFrame.Content is ConsumerControlPage page)
                {
                    var row = new StackPanel { Orientation = Orientation.Horizontal, Spacing = 12 };
                    row.Children.Add(new TextBlock { Text = ingestError.IngestedAt.ToLocalTime().ToString("HH:mm:ss"), Width = 70 });
                    row.Children.Add(new TextBlock { Text = "-", Width = 110 });
                    row.Children.Add(new TextBlock { Text = "ERROR", Width = 70, Foreground = new SolidColorBrush(Microsoft.UI.Colors.IndianRed) });
                    row.Children.Add(new TextBlock { Text = $"{ingestError.ErrorType}: {ingestError.ErrorMessage}", TextWrapping = TextWrapping.WrapWholeWords });

                    page.RecentEventsHost.Children.Add(row);

                    const int maxRows = 200;
                    while (page.RecentEventsHost.Children.Count > maxRows)
                        page.RecentEventsHost.Children.RemoveAt(page.RecentEventsHost.Children.Count - 1);
                }
            });
        }


        private void UpdatePartitionOffsetLabels(int partition, long offset)
        {
            LogToRichTextBox($"Offset -> Partition: {partition}, Offset: {offset}");
            DispatcherQueue?.TryEnqueue(() =>
            {
                if (contentFrame.Content is ConsumerControlPage consumer)
                {
                    consumer.PartitionLabel.Text = $"Current Partition: {partition}";
                    consumer.OffsetLabel.Text = $"Current Offset: {offset}";
                }
            });
        }

        private void UiUpdateTimer_Tick(object? sender, object e)
        {
            var elapsed = DateTime.UtcNow - _lastUiUpdateTime;
            if (elapsed.TotalSeconds > 0)
            {
                double mps = _messagesProcessedCount / elapsed.TotalSeconds;
                DispatcherQueue?.TryEnqueue(() =>
                {
                    if (contentFrame.Content is ConsumerControlPage consumer)
                        consumer.MpsLabel.Text = mps.ToString("F2");
                });
            }
            _messagesProcessedCount = 0;
            _lastUiUpdateTime = DateTime.UtcNow;
        }

        private void btnLogClear_Click(object sender, RoutedEventArgs e)
        {
            systemLog.LogTextBox.Text = string.Empty;
        }

        private void contentFrame_Navigated(object sender, NavigationEventArgs e)
        {
            WirePageHandlers();
        }

        private void navView_SelectionChanged(NavigationView sender, NavigationViewSelectionChangedEventArgs args)
        {
            if (args.SelectedItem is NavigationViewItem item && item.Tag is string tag)
                NavigateTo(tag);
        }

        private void NavigateTo(string tag)
        {
            if (string.IsNullOrWhiteSpace(tag))
                return;

            if (_activeNavTag == tag && contentFrame.Content != null)
                return;

            switch (tag)
            {
                case "consumer": contentFrame.Navigate(typeof(ConsumerControlPage)); break;
                case "config": contentFrame.Navigate(typeof(ConfigurationPage)); break;
                case "query": contentFrame.Navigate(typeof(DataQueryPage)); break;
                default: return;
            }

            _activeNavTag = tag;
            UpdateRailSelection(tag);
        }

        private void UpdateRailSelection(string tag)
        {
            if (_navRailButtons.Count == 0)
                return;

            var activeBackground = Application.Current.Resources["Brush.Nav.ActiveBackground"] as Brush
                                   ?? new SolidColorBrush(Microsoft.UI.Colors.DodgerBlue);
            var inactiveForeground = Application.Current.Resources["Brush.Nav.Inactive"] as Brush
                                     ?? new SolidColorBrush(Microsoft.UI.Colors.LightGray);
            var activeForeground = Application.Current.Resources["Brush.Nav.ActiveForeground"] as Brush
                                   ?? new SolidColorBrush(Microsoft.UI.Colors.White);

            foreach (var kvp in _navRailButtons)
            {
                var button = kvp.Value;
                if (button is null)
                    continue;

                bool isActive = kvp.Key == tag;

                button.Background = isActive ? activeBackground : _navTransparentBrush;
                button.Foreground = isActive ? activeForeground : inactiveForeground;
            }
        }

        private void WirePageHandlers()
        {
            secondaryMenu.Children.Clear();

            if (contentFrame.Content is ConsumerControlPage consumer)
            {
                var title = new TextBlock { Text = "Consumer Control", Margin = new Thickness(4, 0, 0, 10), FontSize = 14, FontWeight = Microsoft.UI.Text.FontWeights.Bold };

                var startButton = new Button
                {
                    Content = "Start",
                    Background = new SolidColorBrush(Microsoft.UI.ColorHelper.FromArgb(255, 15, 112, 224)),
                    Foreground = new SolidColorBrush(Microsoft.UI.Colors.White),
                    CornerRadius = new CornerRadius(8),
                    Padding = new Thickness(12, 4, 12, 4),
                    Margin = new Thickness(0, 0, 8, 0),
                    HorizontalAlignment = HorizontalAlignment.Left
                };
                startButton.Click += buttonStartConsumer_Click;

                var stopButton = new Button
                {
                    Content = "Stop",
                    Background = new SolidColorBrush(Microsoft.UI.ColorHelper.FromArgb(255, 236, 239, 245)),
                    Foreground = new SolidColorBrush(Microsoft.UI.Colors.Black),
                    CornerRadius = new CornerRadius(8),
                    Padding = new Thickness(12, 4, 12, 4),
                    HorizontalAlignment = HorizontalAlignment.Left
                };
                stopButton.Click += buttonStopConsumer_Click;

                var testProducerButton = new Button
                {
                    Content = "Send Test",
                    Background = new SolidColorBrush(Microsoft.UI.ColorHelper.FromArgb(255, 15, 112, 224)),
                    Foreground = new SolidColorBrush(Microsoft.UI.Colors.White),
                    CornerRadius = new CornerRadius(8),
                    Padding = new Thickness(31, 4, 31, 4),
                    Margin = new Thickness(0, 8, 0, 0),
                    HorizontalAlignment = HorizontalAlignment.Left
                };
                testProducerButton.Click += async (s, e) =>
                {
                    try
                    {
                        testProducerButton.IsEnabled = false;
                        string bootstrap = AppSettingsManager.KafkaBootstrapServers ?? "localhost:9092";
                        string topic = AppSettingsManager.KafkaTopic ?? "sensor.events";
                        var producer = new KafkaProducerService(bootstrap, topic);
                        LogToRichTextBox("Sending 3 test messages...");
                        await producer.SendTestMessagesAsync();
                        LogToRichTextBox("✅ Sent 3 test messages successfully!");
                    }
                    catch (Exception ex)
                    {
                        LogErrorToRichTextBox($"Failed to send test messages: {ex.Message}");
                    }
                    finally
                    {
                        testProducerButton.IsEnabled = true;
                    }
                };

                var rowTop = new StackPanel { Orientation = Orientation.Horizontal, Spacing = 6 };
                rowTop.Children.Add(startButton);
                rowTop.Children.Add(stopButton);

                var mainStack = new StackPanel { Orientation = Orientation.Vertical, Spacing = 6, HorizontalAlignment = HorizontalAlignment.Left };
                mainStack.Children.Add(rowTop);
                mainStack.Children.Add(testProducerButton);

                secondaryMenu.Children.Add(title);
                secondaryMenu.Children.Add(mainStack);

                // 페이지 로드 완료 후 전역 저장소로부터 재구성
                RoutedEventHandler? handler = null;
                handler = (s, e) =>
                {
                    consumer.Loaded -= handler;
                    RebuildRecentFromStore(consumer);
                };
                consumer.Loaded -= handler;
                consumer.Loaded += handler;
            }
            else if (contentFrame.Content is ConfigurationPage config)
            {
                var title = new TextBlock { Text = "Configuration", Margin = new Thickness(4, 0, 0, 10), FontSize = 14, FontWeight = Microsoft.UI.Text.FontWeights.Bold };

                config.KafkaBootstrap.Text = AppSettingsManager.KafkaBootstrapServers ?? "localhost:9092";
                config.KafkaBootstrap.SelectionStart = config.KafkaBootstrap.Text.Length;
                config.KafkaBootstrap.SelectionLength = 0;

                config.KafkaTopic.Text = AppSettingsManager.KafkaTopic ?? "sensor.events";
                config.KafkaTopic.SelectionStart = config.KafkaTopic.Text.Length;
                config.KafkaTopic.SelectionLength = 0;

                config.KafkaGroupId.Text = AppSettingsManager.KafkaGroupId ?? "sensor-dashboard-local";
                config.KafkaGroupId.SelectionStart = config.KafkaGroupId.Text.Length;
                config.KafkaGroupId.SelectionLength = 0;

                config.ConnectionStringBox.Text = AppSettingsManager.ConnectionString ?? "Server=(localdb)\\MSSQLLocalDB;Database=SensorDb;Trusted_Connection=True;";
                config.ConnectionStringBox.SelectionStart = config.ConnectionStringBox.Text.Length;
                config.ConnectionStringBox.SelectionLength = 0;

                var testButton = new Button
                {
                    Content = "Test Connection",
                    Margin = new Thickness(0, 0, 0, 0),
                    MinWidth = 140,
                    Height = 36,
                    Background = new SolidColorBrush(Microsoft.UI.ColorHelper.FromArgb(255, 15, 112, 224)),
                    Foreground = new SolidColorBrush(Microsoft.UI.Colors.White),
                    CornerRadius = new CornerRadius(8),
                    Padding = new Thickness(12, 4, 12, 4),
                    HorizontalAlignment = HorizontalAlignment.Left,
                    VerticalAlignment = VerticalAlignment.Center
                };
                testButton.Click += buttonTestConnection_Click;

                secondaryMenu.Children.Add(title);
                secondaryMenu.Children.Add(testButton);
            }
            else if (contentFrame.Content is DataQueryPage q)
            {
                var title = new TextBlock { Text = "Data Query", Margin = new Thickness(4, 0, 0, 10), FontSize = 14, FontWeight = Microsoft.UI.Text.FontWeights.Bold };

                var startLabel = new TextBlock { Text = "Start Date", FontWeight = Microsoft.UI.Text.FontWeights.SemiBold };
                var startTextBox = new TextBox { Width = 250, PlaceholderText = "MM/DD/YYYY" };

                var endLabel = new TextBlock { Text = "End Date", Margin = new Thickness(0, 10, 0, 0), FontWeight = Microsoft.UI.Text.FontWeights.SemiBold };
                var endTextBox = new TextBox { Width = 250, PlaceholderText = "MM/DD/YYYY" };

                var keywordLabel = new TextBlock { Text = "Keyword", Margin = new Thickness(0, 10, 0, 0), FontWeight = Microsoft.UI.Text.FontWeights.SemiBold };
                var keyword = new TextBox { Width = 250 };

                var statusLabel = new TextBlock { Text = "Status", Margin = new Thickness(0, 10, 0, 0), FontWeight = Microsoft.UI.Text.FontWeights.SemiBold };
                var status = new ComboBox { Width = 250 };
                status.Items.Add(new ComboBoxItem { Content = "ALL" });
                status.Items.Add(new ComboBoxItem { Content = "OK" });
                status.Items.Add(new ComboBoxItem { Content = "ERROR" });

                startTextBox.Text = q.StartDatePicker.Date.ToString("MM/dd/yyyy");
                endTextBox.Text = q.EndDatePicker.Date.ToString("MM/dd/yyyy");
                keyword.Text = q.KeywordBox.Text;
                status.SelectedIndex = q.StatusCombo.SelectedIndex;

                startTextBox.TextChanged += (_, __) =>
                {
                    if (DateTime.TryParse(startTextBox.Text, out var s)) q.StartDatePicker.Date = s;
                };
                endTextBox.TextChanged += (_, __) =>
                {
                    if (DateTime.TryParse(endTextBox.Text, out var s)) q.EndDatePicker.Date = s;
                };
                keyword.TextChanged += (_, __) => q.KeywordBox.Text = keyword.Text;
                status.SelectionChanged += (_, __) => q.StatusCombo.SelectedIndex = status.SelectedIndex;

                var searchBtn = new Button
                {
                    Content = "Search",
                    Margin = new Thickness(0, 12, 0, 0),
                    HorizontalAlignment = HorizontalAlignment.Left,
                    VerticalAlignment = VerticalAlignment.Top,
                    HorizontalContentAlignment = HorizontalAlignment.Left,
                    Background = new SolidColorBrush(Microsoft.UI.ColorHelper.FromArgb(255, 15, 112, 224)),
                    Foreground = new SolidColorBrush(Microsoft.UI.Colors.White),
                    CornerRadius = new CornerRadius(8),
                    Padding = new Thickness(12, 4, 12, 4),
                    MinWidth = 0
                };
                searchBtn.Click += buttonQuerySearch_Click;

                secondaryMenu.Children.Add(title);
                secondaryMenu.Children.Add(startLabel);
                secondaryMenu.Children.Add(startTextBox);
                secondaryMenu.Children.Add(endLabel);
                secondaryMenu.Children.Add(endTextBox);
                secondaryMenu.Children.Add(keywordLabel);
                secondaryMenu.Children.Add(keyword);
                secondaryMenu.Children.Add(statusLabel);
                secondaryMenu.Children.Add(status);
                secondaryMenu.Children.Add(searchBtn);

                q.FirstButton.Click -= buttonQueryFirst_Click;
                q.PrevButton.Click -= buttonQueryPrev_Click;
                q.NextButton.Click -= buttonQueryNext_Click;
                q.LastButton.Click -= buttonQueryLast_Click;
                q.SearchButton.Click -= buttonQuerySearch_Click;

                q.FirstButton.Click += buttonQueryFirst_Click;
                q.PrevButton.Click += buttonQueryPrev_Click;
                q.NextButton.Click += buttonQueryNext_Click;
                q.LastButton.Click += buttonQueryLast_Click;
                q.SearchButton.Click += buttonQuerySearch_Click;
            }
        }

        // start
        private async void buttonStartConsumer_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (_kafkaConsumerService != null && _kafkaConsumerService.IsInitialized)
                {
                    if (_kafkaConsumerService.IsPaused)
                    {
                        _kafkaConsumerService.Resume();
                        LogToRichTextBox("Kafka Consumer resumed (fast).");
                        return;
                    }
                    LogToRichTextBox("Kafka Consumer already running.");
                    return;
                }

                string bootstrapServers = AppSettingsManager.KafkaBootstrapServers ?? "";
                string topic = AppSettingsManager.KafkaTopic ?? "";
                string groupId = AppSettingsManager.KafkaGroupId ?? "";
                string connectionString = AppSettingsManager.ConnectionString ?? "";

                _sensorEventRepository = new SensorEventRepository(connectionString);
                _sensorEventRepository.LogAction = LogToRichTextBox;

                _kafkaConsumerService = new KafkaConsumerService(bootstrapServers, topic, groupId, _sensorEventRepository);
                _kafkaConsumerService.LogMessage += LogToRichTextBox;
                _kafkaConsumerService.ErrorOccurred += LogErrorToRichTextBox;
                _kafkaConsumerService.PartitionOffsetUpdated += UpdatePartitionOffsetLabels;
                _kafkaConsumerService.ValidMessageProcessed += HandleValidMessageProcessed;
                _kafkaConsumerService.InvalidMessageProcessed += HandleInvalidMessageProcessed;

                await _kafkaConsumerService.StartAsync();
                LogToRichTextBox("Kafka Consumer started.");
            }
            catch (Exception ex)
            {
                LogErrorToRichTextBox($"Failed to start/resume Kafka Consumer: {ex.Message}");
            }
        }

        // stop
        private void buttonStopConsumer_Click(object sender, RoutedEventArgs e)
        {
            if (_kafkaConsumerService?.IsInitialized == true)
            {
                _kafkaConsumerService.Pause();
                LogToRichTextBox("Kafka Consumer paused (soft stop).");
            }
            else
            {
                _kafkaConsumerService?.Stop();
                LogToRichTextBox("Kafka Consumer stopped.");
            }
        }

        private async void buttonTestConnection_Click(object sender, RoutedEventArgs e)
        {
            string currentConnectionString = AppSettingsManager.ConnectionString ?? "Server=(localdb)\\MSSQLLocalDB;Database=SensorDb;Trusted_Connection=True;";
            if (contentFrame.Content is ConfigurationPage cfg)
                currentConnectionString = cfg.ConnectionStringBox.Text;

            LogToRichTextBox("Testing database connection...");
            try
            {
                using var connection = new Microsoft.Data.SqlClient.SqlConnection(currentConnectionString);
                await connection.OpenAsync();
                LogToRichTextBox("Database connection successful!");
                connection.Close();
            }
            catch (Exception ex)
            {
                LogErrorToRichTextBox($"Database connection failed: {ex.Message}");
            }
        }

        private async void buttonQuerySearch_Click(object sender, RoutedEventArgs e)
        {
            _currentPage = 1;
            await ExecuteQueryFromPage();
        }

        private async void buttonQueryFirst_Click(object sender, RoutedEventArgs e)
        {
            _currentPage = 1;
            await ExecuteQueryFromPage();
        }

        private async void buttonQueryPrev_Click(object sender, RoutedEventArgs e)
        {
            if (_currentPage > 1)
            {
                _currentPage--;
                await ExecuteQueryFromPage();
            }
        }

        private async void buttonQueryNext_Click(object sender, RoutedEventArgs e)
        {
            if (_currentPage < _totalQueryPages)
            {
                _currentPage++;
                await ExecuteQueryFromPage();
            }
        }

        private async void buttonQueryLast_Click(object sender, RoutedEventArgs e)
        {
            _currentPage = _totalQueryPages;
            await ExecuteQueryFromPage();
        }

        private async Task ExecuteQueryFromPage()
        {
            if (_sensorEventRepository == null)
            {
                LogErrorToRichTextBox("SensorEventRepository is not initialized.");
                return;
            }
            if (contentFrame.Content is not DataQueryPage q)
            {
                LogErrorToRichTextBox("DataQuery page not active.");
                return;
            }

            DateTime startTime = q.StartDatePicker.Date.DateTime.Date + q.StartTimePicker.Time;
            DateTime endTime = q.EndDatePicker.Date.DateTime.Date + q.EndTimePicker.Time;

            var statusItem = q.StatusCombo.SelectedItem as ComboBoxItem;
            var statusText = (statusItem?.Content?.ToString() ?? "ALL").Trim().ToUpperInvariant();

            var parameters = new QueryParameters
            {
                StartTime = startTime,
                EndTime = endTime,
                Keyword = q.KeywordBox.Text ?? string.Empty,
                StatusFilter = statusText,
                PageNumber = _currentPage,
                PageSize = _pageSize
            };

            try
            {
                var result = await _sensorEventRepository.SearchEventsAsync(parameters);

                q.ResultsList.ItemsSource = new ObservableCollection<object>(result.Items);
                _totalQueryPages = result.TotalPages;
                _currentPage = result.CurrentPage;

                q.PageInfo.Text = $"Page {_currentPage} of {_totalQueryPages}";
                q.PrevButton.IsEnabled = (_currentPage > 1);
                q.FirstButton.IsEnabled = (_currentPage > 1);
                q.NextButton.IsEnabled = (_currentPage < _totalQueryPages);
                q.LastButton.IsEnabled = (_currentPage < _totalQueryPages);
            }
            catch (Exception ex)
            {
                LogErrorToRichTextBox($"Query execution failed: {ex.Message}");
            }
        }

        private void btnSecControls_Click(object sender, RoutedEventArgs e) { }
        private void btnSecStatus_Click(object sender, RoutedEventArgs e)
        {
            if (contentFrame.Content is ConsumerControlPage page)
                page.StatusSection.StartBringIntoView(new BringIntoViewOptions { AnimationDesired = true });
        }
        private void btnSecRecent_Click(object sender, RoutedEventArgs e)
        {
            if (contentFrame.Content is ConsumerControlPage page)
                page.RecentSection.StartBringIntoView(new BringIntoViewOptions { AnimationDesired = true });
        }

        private void railNav_Click(object sender, RoutedEventArgs e)
        {
            if (sender is Button b && b.Tag is string tag)
            {
                NavigateTo(tag);
            }
        }
    }
}
