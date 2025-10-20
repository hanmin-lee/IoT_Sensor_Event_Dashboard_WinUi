using System;
using System.Collections.Generic;
using System.Collections.ObjectModel; // ObservableCollection을 위해 추가
using System.ComponentModel;
using System.Linq;
using System.Threading; // Interlocked를 위해 추가
using System.Threading.Tasks; // Task 사용을 위해 추가
using Confluent.Kafka;
using Microsoft.Data.SqlClient;
using Microsoft.UI.Dispatching;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Controls.Primitives;
using Microsoft.UI.Xaml.Data;
using Microsoft.UI.Xaml.Media;
using Windows.ApplicationModel.Activation;
using Microsoft.UI.Text; // RichEditBox Document 관련 추가
using IoT_Sensor_Event_Dashboard_WinUi.Pages; // added for navigation
using IoT_Sensor_Event_Dashboard_WinUi.Controls;
using Microsoft.UI.Xaml.Navigation; // added for NavigationEventArgs

namespace IoT_Sensor_Event_Dashboard_WinUi
{
    public sealed partial class MainWindow : Window
    {
        private KafkaConsumerService? _kafkaConsumerService;
        private SensorEventRepository? _sensorEventRepository;
        private Microsoft.UI.Xaml.DispatcherTimer? _uiUpdateTimer; // updated type

        private long _messagesProcessedCount = 0;
        private DateTime _lastUiUpdateTime = DateTime.UtcNow;
        private const int MaxLogLines = 5000;

        private int _currentPage = 1;
        private int _pageSize = 20;
        private int _totalQueryPages = 1;

        public MainWindow()
        {
            this.InitializeComponent();
            this.Closed += MainWindow_Closed; // 창 닫기 이벤트 핸들러 구독
            InitializeApplicationComponents();

            SetupListViewColumns();
            SetupQueryResultListViewColumns();
            InitializeQueryTab();

            // new layout: wire system log buttons if present
            try { systemLog.ClearButton.Click += btnLogClear_Click; } catch {}
            // new layout: navigate default page if contentFrame exists
            try { contentFrame.Navigate(typeof(ConsumerControlPage)); } catch {}
        }

        private void MainWindow_Closed(object sender, WindowEventArgs args)
        {
            _kafkaConsumerService?.Stop();
        }

        private void InitializeApplicationComponents()
        {
            // Load settings from AppSettingsManager
            string bootstrapServers = AppSettingsManager.KafkaBootstrapServers ?? "localhost:9092";
            string topic = AppSettingsManager.KafkaTopic ?? "sensor.events";
            string groupId = AppSettingsManager.KafkaGroupId ?? "sensor-dashboard-local";
            string connectionString = AppSettingsManager.ConnectionString ?? "Server=(localdb)\\MSSQLLocalDB;Database=SensorDb;Trusted_Connection=True;";

            // Initialize repository and service
            _sensorEventRepository = new SensorEventRepository(connectionString);
            _sensorEventRepository.LogAction = LogToRichTextBox; // Pass the logging action

            _kafkaConsumerService = new KafkaConsumerService(bootstrapServers, topic, groupId, _sensorEventRepository);

            // Wire up logging from KafkaConsumerService to TextBox
            _kafkaConsumerService.LogMessage += LogToRichTextBox;
            _kafkaConsumerService.ErrorOccurred += LogErrorToRichTextBox;
            _kafkaConsumerService.PartitionOffsetUpdated += UpdatePartitionOffsetLabels;

            // Initialize UI update timer for MPS
            _uiUpdateTimer = new Microsoft.UI.Xaml.DispatcherTimer();
            _uiUpdateTimer.Interval = TimeSpan.FromSeconds(1); // 1 second
            _uiUpdateTimer.Tick += UiUpdateTimer_Tick;
            _uiUpdateTimer.Start();

            LogToRichTextBox("Application initialized.");
            LogToRichTextBox($"Loaded Kafka Bootstrap Servers: {bootstrapServers}");
            LogToRichTextBox($"Loaded Kafka Topic: {topic}");
            LogToRichTextBox($"Loaded Kafka Group ID: {groupId}");
            LogToRichTextBox($"Loaded DB Connection String (partial): {connectionString.Substring(0, Math.Min(50, connectionString.Length))}...");

            // App.xaml.cs에서 Closing 이벤트 핸들러를 MainWindow_Closed로 변경했으므로,
            // 여기서는 _window.Closed += MainWindow_Closed; 를 제거합니다.
        }

        private void EnsureMaxLogLines()
        {
            // Keep only the last MaxLogLines lines by scanning from the end
            string text = systemLog.LogTextBox.Text ?? string.Empty;
            int newlineSeen = 0;
            for (int i = text.Length - 1; i >= 0; i--)
            {
                if (text[i] == '\n')
                {
                    newlineSeen++;
                    if (newlineSeen > MaxLogLines)
                    {
                        systemLog.LogTextBox.Text = text.Substring(i + 1);
                        break;
                    }
                }
            }
        }

        private void AppendLog(string line)
        {
            try {
                systemLog.LogTextBox.Text += line + "\n";
                EnsureMaxLogLines();
                systemLog.LogTextBox.SelectionStart = systemLog.LogTextBox.Text.Length;
                systemLog.LogTextBox.SelectionLength = 0;
            } catch {}
        }

        // LogToTextBox 메서드 (MainForm.cs에서 이동, WinUI TextBox에 맞게 수정)
        private void LogToRichTextBox(string message)
        {
            if (this.DispatcherQueue != null)
            {
                this.DispatcherQueue.TryEnqueue(() =>
                {
                    AppendLog($"[{DateTime.Now:HH:mm:ss}] {message}");
                });
            }
        }

        // LogErrorToTextBox 메서드 (MainForm.cs에서 이동, WinUI TextBox에 맞게 수정)
        private void LogErrorToRichTextBox(string errorMessage)
        {
            if (this.DispatcherQueue != null)
            {
                this.DispatcherQueue.TryEnqueue(() =>
                {
                    AppendLog($"[{DateTime.Now:HH:mm:ss}] ERROR: {errorMessage}");
                });
            }
        }

        private void SetupListViewColumns()
        {
            // ListView의 ItemTemplate을 코드에서 동적으로 생성하는 것은 복잡.
            // XAML에서 DataTemplate을 정의하고, 여기서는 ItemTemplate을 설정하는 것으로 대체.
            // 또는 Microsoft.Toolkit.Uwp.UI.Controls.DataGrid를 사용하는 것이 더 적합함.
            // 여기서는 임시로 ItemTemplate을 설정하는 대신, 나중에 XAML에서 DataTemplate을 정의하는 것을 권장.
            // 아니면 ViewModel을 사용하고 Binding하는 것이 WinUI의 일반적인 방식.

            // 임시로, ListView에 표시될 RecentEventItem의 속성들을 TextBlock으로 나열하는 DataTemplate을 XAML에서 정의했다고 가정.
            // 예시: <ListView.ItemTemplate>
            //          <DataTemplate x:DataType="local:RecentEventItem">
            //              <StackPanel Orientation="Horizontal">
            //                  <TextBlock Text="{x:Bind Type}" Width="80" Margin="0,0,10,0"/>
            //                  <TextBlock Text="{x:Bind DeviceId}" Width="100" Margin="0,0,10,0"/>
            //                  <TextBlock Text="{x:Bind EventTime}" Width="150" Margin="0,0,10,0"/>
            //                  <TextBlock Text="{x:Bind Status}" Width="80" Margin="0,0,10,0"/>
            //                  <TextBlock Text="{x:Bind Message}" HorizontalAlignment="Stretch"/>
            //              </StackPanel>
            //          </DataTemplate>
            //       </ListView.ItemTemplate>

            // 코드 비하인드에서 ListViewHeader를 설정하는 것은 WinForms DataGridView와 다름.
            // GridViewHeaderRowPresenter (UWP) 같은 컨트롤을 사용하거나, DataGrid 사용이 더 편리함.
            // 현재 ListView는 헤더를 직접적으로 지원하지 않음. ColumnHeader를 흉내내는 StackPanel을 XAML에 추가하는 것을 권장.
        }

        private void SetupQueryResultListViewColumns()
        {
            // SetupListViewColumns와 유사하게 처리.
            // XAML에서 DataTemplate 정의를 통해 DataGridView의 컬럼처럼 보이도록 할 수 있음.
        }

        private void InitializeQueryTab() { }

        private void HandleValidMessageProcessed(ConsumeResult<Ignore, string> consumeResult, SensorEvent sensorEvent)
        {
            Interlocked.Increment(ref _messagesProcessedCount);
            LogToRichTextBox($"Valid event: {sensorEvent.DeviceId} {sensorEvent.EventTime:HH:mm:ss} {sensorEvent.Status}");

            // Also render into Recent Events on the Consumer page
            if (this.DispatcherQueue != null)
            {
                this.DispatcherQueue.TryEnqueue(() =>
                {
                    if (contentFrame.Content is ConsumerControlPage page)
                    {
                        var row = new StackPanel { Orientation = Orientation.Horizontal, Spacing = 12 };
                        row.Children.Add(new TextBlock { Text = sensorEvent.EventTime.ToLocalTime().ToString("HH:mm:ss"), Width = 70 });
                        row.Children.Add(new TextBlock { Text = sensorEvent.DeviceId, Width = 110 });
                        row.Children.Add(new TextBlock { Text = sensorEvent.Status, Width = 70 });
                        var msg = new TextBlock { Text = sensorEvent.Message ?? string.Empty };
                        row.Children.Add(msg);

                        // Insert newest at top
                        page.RecentEventsHost.Children.Insert(0, row);

                        // Trim to last 200 rows to avoid unbounded growth
                        const int maxRows = 200;
                        while (page.RecentEventsHost.Children.Count > maxRows)
                        {
                            page.RecentEventsHost.Children.RemoveAt(page.RecentEventsHost.Children.Count - 1);
                        }
                    }
                });
            }
        }

        private void HandleInvalidMessageProcessed(ConsumeResult<Ignore, string> consumeResult, IngestError ingestError)
        {
            Interlocked.Increment(ref _messagesProcessedCount);
            LogErrorToRichTextBox($"Invalid message at {ingestError.OffsetValue}: {ingestError.ErrorType}");

            // Also render invalid messages into Recent Events with visual distinction
            if (this.DispatcherQueue != null)
            {
                this.DispatcherQueue.TryEnqueue(() =>
                {
                    if (contentFrame.Content is ConsumerControlPage page)
                    {
                        var row = new StackPanel { Orientation = Orientation.Horizontal, Spacing = 12 };
                        row.Children.Add(new TextBlock { Text = ingestError.IngestedAt.ToLocalTime().ToString("HH:mm:ss"), Width = 70 });
                        row.Children.Add(new TextBlock { Text = "-", Width = 110 });
                        row.Children.Add(new TextBlock { Text = "ERROR", Width = 70, Foreground = new SolidColorBrush(Microsoft.UI.Colors.IndianRed) });
                        var msg = new TextBlock { Text = $"{ ingestError.ErrorType }: { ingestError.ErrorMessage }", TextWrapping = TextWrapping.WrapWholeWords };
                        row.Children.Add(msg);

                        page.RecentEventsHost.Children.Insert(0, row);

                        const int maxRows = 200;
                        while (page.RecentEventsHost.Children.Count > maxRows)
                        {
                            page.RecentEventsHost.Children.RemoveAt(page.RecentEventsHost.Children.Count - 1);
                        }
                    }
                });
            }
        }

        private void UpdatePartitionOffsetLabels(int partition, long offset)
        {
            LogToRichTextBox($"Offset -> Partition: {partition}, Offset: {offset}");
            if (this.DispatcherQueue != null)
            {
                this.DispatcherQueue.TryEnqueue(() =>
                {
                    if (contentFrame.Content is ConsumerControlPage consumer)
                    {
                        consumer.PartitionLabel.Text = $"Current Partition: {partition}";
                        consumer.OffsetLabel.Text = $"Current Offset: {offset}";
                    }
                });
            }
        }

        private void UiUpdateTimer_Tick(object? sender, object e)
        {
            TimeSpan elapsed = DateTime.UtcNow - _lastUiUpdateTime;
            if (elapsed.TotalSeconds > 0)
            {
                double mps = _messagesProcessedCount / elapsed.TotalSeconds;
                // Messages/Sec 로그 출력 제거 - UI에만 표시
                if (this.DispatcherQueue != null)
                {
                    this.DispatcherQueue.TryEnqueue(() =>
                    {
                        if (contentFrame.Content is ConsumerControlPage consumer)
                        {
                            consumer.MpsLabel.Text = mps.ToString("F2");
                        }
                    });
                }
            }
            _messagesProcessedCount = 0; // Reset for next interval
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
            {
                NavigateTo(tag);
            }
        }

        private void NavigateTo(string tag)
        {
            switch (tag)
            {
                case "consumer":
                    contentFrame.Navigate(typeof(ConsumerControlPage));
                    break;
                case "config":
                    contentFrame.Navigate(typeof(ConfigurationPage));
                    break;
                case "query":
                    contentFrame.Navigate(typeof(DataQueryPage));
                    break;
            }
        }

        private void WirePageHandlers()
        {
            secondaryMenu.Children.Clear();
            if (contentFrame.Content is ConsumerControlPage consumer)
            {
                var title = new TextBlock { Text = "Consumer Control", Margin = new Thickness(4, 0, 0, 10), FontSize = 14, FontWeight = Microsoft.UI.Text.FontWeights.Bold };
                // Start 버튼을 Button으로 생성
                var startButton = new Button();
                startButton.Content = "start";
                startButton.Background = new SolidColorBrush(Microsoft.UI.ColorHelper.FromArgb(255, 15, 112, 224)); // #0F70E0
                startButton.Foreground = new SolidColorBrush(Microsoft.UI.Colors.White);
                startButton.CornerRadius = new CornerRadius(8);
                startButton.Padding = new Thickness(12,4,12,4);
                startButton.Margin = new Thickness(0,0,8,0);
                startButton.MinWidth = 0;
                startButton.HorizontalAlignment = HorizontalAlignment.Center;
                startButton.VerticalAlignment = VerticalAlignment.Center;
                startButton.Click += buttonStartConsumer_Click;
                // hover/press visuals
                var startBase = Microsoft.UI.ColorHelper.FromArgb(255, 15, 112, 224);   // #0F70E0
                var startHover = Microsoft.UI.ColorHelper.FromArgb(255, 43, 116, 240);  // #2B74F0
                var startPressed = Microsoft.UI.ColorHelper.FromArgb(255, 30, 77, 179); // #1E4DB3
                startButton.PointerEntered += (_, __) => startButton.Background = new SolidColorBrush(startHover);
                startButton.PointerExited += (_, __) => startButton.Background = new SolidColorBrush(startBase);
                startButton.PointerPressed += (_, __) => startButton.Background = new SolidColorBrush(startPressed);
                startButton.PointerReleased += (_, __) => startButton.Background = new SolidColorBrush(startHover);

                // Stop 버튼을 Button으로 생성
                var stopButton = new Button();
                stopButton.Content = "stop";
                stopButton.Background = new SolidColorBrush(Microsoft.UI.ColorHelper.FromArgb(255, 236, 239, 245)); // #ECEFF5
                stopButton.Foreground = new SolidColorBrush(Microsoft.UI.Colors.Black);
                stopButton.CornerRadius = new CornerRadius(8);
                stopButton.Padding = new Thickness(12,4,12,4);
                stopButton.MinWidth = 0;
                stopButton.HorizontalAlignment = HorizontalAlignment.Center;
                stopButton.VerticalAlignment = VerticalAlignment.Center;
                stopButton.Click += buttonStopConsumer_Click;
                // hover/press visuals
                var stopBase = Microsoft.UI.ColorHelper.FromArgb(255, 236, 239, 245);  // #ECEFF5
                var stopHover = Microsoft.UI.ColorHelper.FromArgb(255, 226, 232, 240); // #E2E8F0
                var stopPressed = Microsoft.UI.ColorHelper.FromArgb(255, 203, 213, 225); // #CBD5E1
                stopButton.PointerEntered += (_, __) => stopButton.Background = new SolidColorBrush(stopHover);
                stopButton.PointerExited += (_, __) => stopButton.Background = new SolidColorBrush(stopBase);
                stopButton.PointerPressed += (_, __) => stopButton.Background = new SolidColorBrush(stopPressed);
                stopButton.PointerReleased += (_, __) => stopButton.Background = new SolidColorBrush(stopHover);

                var row = new StackPanel { Orientation = Orientation.Horizontal, Spacing = 6, HorizontalAlignment = HorizontalAlignment.Center };
                row.Children.Add(startButton);
                row.Children.Add(stopButton);
                secondaryMenu.Children.Add(title);
                secondaryMenu.Children.Add(row);
            }
            else if (contentFrame.Content is ConfigurationPage config)
            {
                var title = new TextBlock { Text = "Configuration", Margin = new Thickness(4, 0, 0, 10), FontSize = 14, FontWeight = Microsoft.UI.Text.FontWeights.Bold };
                
                // Configuration 페이지의 텍스트박스에 현재 설정값 설정
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
                    HorizontalAlignment = HorizontalAlignment.Center,
                    VerticalAlignment = VerticalAlignment.Center
                };
                // Apply same visual styling as start/search
                testButton.Background = new SolidColorBrush(Microsoft.UI.ColorHelper.FromArgb(255, 15, 112, 224)); // #0F70E0
                testButton.Foreground = new SolidColorBrush(Microsoft.UI.Colors.White);
                testButton.CornerRadius = new CornerRadius(8);
                testButton.Padding = new Thickness(12,4,12,4);
                // Hover/press transitions
                var tcBase = Microsoft.UI.ColorHelper.FromArgb(255, 15, 112, 224);   // #0F70E0
                var tcHover = Microsoft.UI.ColorHelper.FromArgb(255, 43, 116, 240);  // #2B74F0
                var tcPressed = Microsoft.UI.ColorHelper.FromArgb(255, 30, 77, 179); // #1E4DB3
                testButton.PointerEntered += (_, __) => testButton.Background = new SolidColorBrush(tcHover);
                testButton.PointerExited  += (_, __) => testButton.Background = new SolidColorBrush(tcBase);
                testButton.PointerPressed += (_, __) => testButton.Background = new SolidColorBrush(tcPressed);
                testButton.PointerReleased+= (_, __) => testButton.Background = new SolidColorBrush(tcHover);
                testButton.Click += buttonTestConnection_Click;
                
                secondaryMenu.Children.Add(title);
                secondaryMenu.Children.Add(testButton);
            }
            else if (contentFrame.Content is DataQueryPage)
            {
                var title = new TextBlock { Text = "Data Query", Margin = new Thickness(4, 0, 0, 10), FontSize = 14, FontWeight = Microsoft.UI.Text.FontWeights.Bold };

                var startLabel = new TextBlock { Text = "Start Date", FontWeight = Microsoft.UI.Text.FontWeights.SemiBold };
                var startTextBox = new TextBox { Width = 250, PlaceholderText = "MM/DD/YYYY" };
                var endLabel = new TextBlock { Text = "End Date", Margin = new Thickness(0,10,0,0), FontWeight = Microsoft.UI.Text.FontWeights.SemiBold };
                var endTextBox = new TextBox { Width = 250, PlaceholderText = "MM/DD/YYYY" };
                var keywordLabel = new TextBlock { Text = "Keyword", Margin = new Thickness(0,10,0,0), FontWeight = Microsoft.UI.Text.FontWeights.SemiBold };
                var keyword = new TextBox { Width = 250 };
                var statusLabel = new TextBlock { Text = "Status", Margin = new Thickness(0,10,0,0), FontWeight = Microsoft.UI.Text.FontWeights.SemiBold };
                var status = new ComboBox { Width = 250 };
                status.Items.Add(new ComboBoxItem { Content = "All" });
                status.Items.Add(new ComboBoxItem { Content = "OK" });
                status.Items.Add(new ComboBoxItem { Content = "ERROR" });

                var dq = (DataQueryPage)contentFrame.Content;
                // Initialize rail controls from page values
                startTextBox.Text = dq.StartDatePicker.Date.ToString("MM/dd/yyyy");
                endTextBox.Text = dq.EndDatePicker.Date.ToString("MM/dd/yyyy");
                keyword.Text = dq.KeywordBox.Text;
                status.SelectedIndex = dq.StatusCombo.SelectedIndex;

                // Keep page updated when rail changes
                startTextBox.TextChanged += (_, __) => { 
                    if (DateTime.TryParse(startTextBox.Text, out DateTime startDate))
                        dq.StartDatePicker.Date = startDate;
                };
                endTextBox.TextChanged += (_, __) => { 
                    if (DateTime.TryParse(endTextBox.Text, out DateTime endDate))
                        dq.EndDatePicker.Date = endDate;
                };
                keyword.TextChanged += (_, __) => { dq.KeywordBox.Text = keyword.Text; };
                status.SelectionChanged += (_, __) => { dq.StatusCombo.SelectedIndex = status.SelectedIndex; };

                var searchBtn = new Button
                {
                    Content = "Search",
                    Margin = new Thickness(0, 12, 0, 0),
                    HorizontalAlignment = HorizontalAlignment.Left,      // ← 꽉 채우기
                    VerticalAlignment = VerticalAlignment.Top,
                    HorizontalContentAlignment = HorizontalAlignment.Left    // ← 내용은 왼쪽 정렬
                };

                searchBtn.Background = new SolidColorBrush(Microsoft.UI.ColorHelper.FromArgb(255, 15, 112, 224)); // #0F70E0
                searchBtn.Foreground = new SolidColorBrush(Microsoft.UI.Colors.White);
                searchBtn.CornerRadius = new CornerRadius(8);
                searchBtn.Padding = new Thickness(12,4,12,4);
                searchBtn.MinWidth = 0;
                // hover/press visuals to match start button
                var searchBase = Microsoft.UI.ColorHelper.FromArgb(255, 15, 112, 224);   // #0F70E0
                var searchHover = Microsoft.UI.ColorHelper.FromArgb(255, 43, 116, 240);  // #2B74F0
                var searchPressed = Microsoft.UI.ColorHelper.FromArgb(255, 30, 77, 179); // #1E4DB3
                searchBtn.PointerEntered += (_, __) => searchBtn.Background = new SolidColorBrush(searchHover);
                searchBtn.PointerExited +=  (_, __) => searchBtn.Background = new SolidColorBrush(searchBase);
                searchBtn.PointerPressed += (_, __) => searchBtn.Background = new SolidColorBrush(searchPressed);
                searchBtn.PointerReleased += (_, __) => searchBtn.Background = new SolidColorBrush(searchHover);
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
            }
        }

        private async void buttonStartConsumer_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (_kafkaConsumerService != null)
                {
                    _kafkaConsumerService.Stop();
                    _kafkaConsumerService.LogMessage -= LogToRichTextBox;
                    _kafkaConsumerService.ErrorOccurred -= LogErrorToRichTextBox;
                    _kafkaConsumerService.PartitionOffsetUpdated -= UpdatePartitionOffsetLabels;
                    _kafkaConsumerService.ValidMessageProcessed -= HandleValidMessageProcessed;
                    _kafkaConsumerService.InvalidMessageProcessed -= HandleInvalidMessageProcessed;
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
                _kafkaConsumerService.Start();
                LogToRichTextBox("Kafka Consumer started.");
            }
            catch (Exception ex)
            {
                LogErrorToRichTextBox($"Failed to start Kafka Consumer: {ex.Message}");
            }
        }

        private void buttonStopConsumer_Click(object sender, RoutedEventArgs e)
        {
            _kafkaConsumerService?.Stop();
            LogToRichTextBox("Kafka Consumer stopped.");
        }

        private async void buttonTestConnection_Click(object sender, RoutedEventArgs e)
        {
            string currentConnectionString = AppSettingsManager.ConnectionString ?? "Server=(localdb)\\MSSQLLocalDB;Database=SensorDb;Trusted_Connection=True;";
            if (contentFrame.Content is ConfigurationPage cfg)
            {
                currentConnectionString = cfg.ConnectionStringBox.Text;
            }

            LogToRichTextBox("Testing database connection...");
            try
            {
                using (var connection = new Microsoft.Data.SqlClient.SqlConnection(currentConnectionString))
                {
                    await connection.OpenAsync();
                    LogToRichTextBox("Database connection successful!");
                    connection.Close();
                }
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

            DateTime startTime = q.StartDatePicker.Date.DateTime.Date;
            var startTimeOfDay = q.StartTimePicker.Time;
            startTime = startTime.Add(startTimeOfDay);

            DateTime endTime = q.EndDatePicker.Date.DateTime.Date;
            var endTimeOfDay = q.EndTimePicker.Time;
            endTime = endTime.Add(endTimeOfDay);

            var parameters = new QueryParameters
            {
                StartTime = startTime,
                EndTime = endTime,
                Keyword = q.KeywordBox.Text ?? string.Empty,
                StatusFilter = (q.StatusCombo.SelectedItem as ComboBoxItem)?.Content?.ToString() ?? "ALL",
                PageNumber = _currentPage,
                PageSize = _pageSize
            };

            try
            {
                var result = await _sensorEventRepository.SearchEventsAsync(parameters);
                q.ResultsList.ItemsSource = new System.Collections.ObjectModel.ObservableCollection<object>(result.Items);
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

        private void btnSecControls_Click(object sender, RoutedEventArgs e)
        {
            // Controls section has been removed; no-op.
        }

        private void btnSecStatus_Click(object sender, RoutedEventArgs e)
        {
            if (contentFrame.Content is ConsumerControlPage page)
            {
                page.StatusSection.StartBringIntoView(new BringIntoViewOptions { AnimationDesired = true });
            }
        }

        private void btnSecRecent_Click(object sender, RoutedEventArgs e)
        {
            if (contentFrame.Content is ConsumerControlPage page)
            {
                page.RecentSection.StartBringIntoView(new BringIntoViewOptions { AnimationDesired = true });
            }
        }

        private void railNav_Click(object sender, RoutedEventArgs e)
        {
            if (sender is Button b && b.Tag is string tag)
            {
                NavigateTo(tag);
                WirePageHandlers();
            }
        }
    }
}
