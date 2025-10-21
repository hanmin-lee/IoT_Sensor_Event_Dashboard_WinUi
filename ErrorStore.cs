// ErrorStore.cs
using System;
using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.UI.Dispatching;

namespace IoT_Sensor_Event_Dashboard_WinUi
{
    /// <summary>
    /// 전역 에러 이벤트 저장소 (UI와 분리)
    /// - 어디서든 Enqueue 하면 내부 큐에 쌓고
    /// - 짧은 주기로 UI 스레드에서 ObservableCollection에 반영
    /// </summary>
    public sealed class ErrorStore
    {
        // 오래된 → 최신 순 (뒤가 최신)
        public ObservableCollection<IngestError> Recent { get; } = new();

        private readonly ConcurrentQueue<IngestError> _queue = new();
        private readonly DispatcherQueue _ui;
        private readonly int _maxItems;
        private readonly TimeSpan _flushInterval;
        private bool _flushing;

        public ErrorStore(DispatcherQueue ui, int maxItems = 2000, int flushMs = 50)
        {
            _ui = ui;
            _maxItems = maxItems;
            _flushInterval = TimeSpan.FromMilliseconds(flushMs);
        }

        public void Enqueue(IngestError err)
        {
            if (err == null) return;
            _queue.Enqueue(err);
            TryScheduleFlush();
        }

        private void TryScheduleFlush()
        {
            if (_flushing) return;
            _flushing = true;

            _ui.TryEnqueue(async () =>
            {
                try
                {
                    await Task.Delay(_flushInterval);

                    while (_queue.TryDequeue(out var e))
                    {
                        Recent.Add(e);
                        if (Recent.Count > _maxItems)
                            Recent.RemoveAt(0);
                    }
                }
                finally
                {
                    _flushing = false;
                }
            });
        }

        public IngestError[] TakeLast(int count)
        {
            int take = Math.Max(0, Math.Min(count, Recent.Count));
            if (take == 0) return Array.Empty<IngestError>();
            return Recent.Skip(Recent.Count - take).ToArray();
        }
    }
}
