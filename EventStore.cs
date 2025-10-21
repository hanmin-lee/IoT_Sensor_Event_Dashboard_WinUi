// EventStore.cs
using System;
using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.UI.Dispatching;

namespace IoT_Sensor_Event_Dashboard_WinUi
{
    /// <summary>
    /// 전역 최근 이벤트 저장소 (UI와 분리)
    /// - 어디서든 Enqueue 하면 내부 큐에 쌓고
    /// - 짧은 주기로 UI 스레드에서 ObservableCollection에 반영
    /// </summary>
    public sealed class EventStore
    {
        // 오래된 → 최신 순 (뒤가 최신)
        public ObservableCollection<SensorEvent> Recent { get; } = new();

        private readonly ConcurrentQueue<SensorEvent> _queue = new();
        private readonly DispatcherQueue _ui;
        private readonly int _maxItems;
        private readonly TimeSpan _flushInterval;
        private bool _flushing;

        public EventStore(DispatcherQueue ui, int maxItems = 2000, int flushMs = 50)
        {
            _ui = ui;
            _maxItems = maxItems;
            _flushInterval = TimeSpan.FromMilliseconds(flushMs);
        }

        /// <summary>백그라운드 호출 OK (UI 마샬링 내부 처리)</summary>
        public void Enqueue(SensorEvent ev)
        {
            if (ev == null) return;
            _queue.Enqueue(ev);
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
                    // 약간 모아서 한 번에 반영 (UI 부하 감소)
                    await Task.Delay(_flushInterval);

                    while (_queue.TryDequeue(out var ev))
                    {
                        Recent.Add(ev);
                        if (Recent.Count > _maxItems)
                            Recent.RemoveAt(0); // 링버퍼 유지
                    }
                }
                finally
                {
                    _flushing = false;
                }
            });
        }

        /// <summary>가장 최근 N개 (최신순 배열 반환)</summary>
        public SensorEvent[] TakeLast(int count)
        {
            int take = Math.Max(0, Math.Min(count, Recent.Count));
            if (take == 0) return Array.Empty<SensorEvent>();
            return Recent.Skip(Recent.Count - take).ToArray();
        }
    }
}
