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

            // �⺻�� ���� (������ ó�� ���� ���� ����)
            StartDatePicker.Date = new DateTimeOffset(new DateTime(2025, 10, 1));
            StartTimePicker.Time = new TimeSpan(0, 0, 0);

            EndDatePicker.Date = new DateTimeOffset(DateTime.Today);
            EndTimePicker.Time = new TimeSpan(23, 59, 59);

            // ������ ������ �ڵ����� ������ ����
            ScheduleMidnightRefresh();
        }

        private DispatcherTimer? _midnightTimer;

        private void ScheduleMidnightRefresh()
        {
            var now = DateTime.Now;
            var nextMidnight = now.Date.AddDays(1);   // ���� ����
            var timeUntilMidnight = nextMidnight - now;

            _midnightTimer = new DispatcherTimer
            {
                Interval = timeUntilMidnight
            };
            _midnightTimer.Tick += (_, __) =>
            {
                // ������ �Ǹ� �������� ���� ��¥�� ����
                EndDatePicker.Date = new DateTimeOffset(DateTime.Today);
                EndTimePicker.Time = new TimeSpan(23, 59, 59);

                // ���� ������ ���� 24�ð� �ֱ�� ��ȯ
                _midnightTimer!.Interval = TimeSpan.FromDays(1);
            };
            _midnightTimer.Start();
        }


    }
}


