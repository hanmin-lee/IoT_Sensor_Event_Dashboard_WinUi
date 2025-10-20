using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml;

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
            this.InitializeComponent();
        }
    }
}


