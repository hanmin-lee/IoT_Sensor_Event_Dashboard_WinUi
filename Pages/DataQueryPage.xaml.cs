using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using System;

namespace IoT_Sensor_Event_Dashboard_WinUi.Pages
{
    public sealed partial class DataQueryPage : Page
    {
        public DatePicker StartDatePicker => datePickerQueryStartTime;
        public TimePicker StartTimePicker => timePickerQueryStartTime;
        public DatePicker EndDatePicker => datePickerQueryEndTime;
        public TimePicker EndTimePicker => timePickerQueryEndTime;
        public TextBox KeywordBox => textBoxQueryKeyword;
        public ComboBox StatusCombo => comboBoxQueryStatus;
        public Button SearchButton => buttonQuerySearch;
        public ListView ResultsList => listViewQueryResult;
        public Button FirstButton => buttonQueryFirst;
        public Button PrevButton => buttonQueryPrev;
        public Button NextButton => buttonQueryNext;
        public Button LastButton => buttonQueryLast;
        public TextBlock PageInfo => labelQueryPageInfo;

        public DataQueryPage()
        {
            InitializeComponent();

            // 기본값 설정 (페이지 처음 열릴 때만 적용)
            StartDatePicker.Date = new DateTimeOffset(new DateTime(2025, 10, 1));
            StartTimePicker.Time = new TimeSpan(0, 0, 0);

            EndDatePicker.Date = new DateTimeOffset(DateTime.Today);
            EndTimePicker.Time = new TimeSpan(23, 59, 59);

            // 자정이 지나면 자동으로 종료일 갱신
            ScheduleMidnightRefresh();
        }

        private DispatcherTimer? _midnightTimer;

        private void ScheduleMidnightRefresh()
        {
            var now = DateTime.Now;
            var nextMidnight = now.Date.AddDays(1);   // 다음 자정
            var timeUntilMidnight = nextMidnight - now;

            _midnightTimer = new DispatcherTimer
            {
                Interval = timeUntilMidnight
            };
            _midnightTimer.Tick += (_, __) =>
            {
                // 자정이 되면 종료일을 오늘 날짜로 갱신
                EndDatePicker.Date = new DateTimeOffset(DateTime.Today);
                EndTimePicker.Time = new TimeSpan(23, 59, 59);

                // 다음 자정을 위해 24시간 주기로 전환
                _midnightTimer!.Interval = TimeSpan.FromDays(1);
            };
            _midnightTimer.Start();
        }


    }
}


