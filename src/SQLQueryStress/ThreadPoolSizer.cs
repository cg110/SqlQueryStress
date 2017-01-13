using System;
using System.Threading;

namespace SQLQueryStress
{
    public sealed class ThreadPoolSizer: IDisposable
    {
        private readonly int _originalMinWorkerSize;
        private readonly int _originalMaxWorkerSize;
        private readonly int _originalMinCompletionSize;
        private readonly int _originalMaxCompletionSize;
        private readonly bool _threadPoolChanged;

        public ThreadPoolSizer(int extraThreads)
        {
            ThreadPool.GetMinThreads(out _originalMinWorkerSize, out _originalMinCompletionSize);
            ThreadPool.GetMaxThreads(out _originalMaxWorkerSize, out _originalMaxCompletionSize);

            var workerthreads = _originalMinWorkerSize;
            var completionThreads = _originalMinCompletionSize;

            if (_originalMinWorkerSize < extraThreads)
            {
                workerthreads += extraThreads;
                _threadPoolChanged = true;
            }
            if (_originalMinCompletionSize < extraThreads)
            {
                completionThreads += extraThreads;
                _threadPoolChanged = true;
            }

            if (_threadPoolChanged)
            {
                var maxWorkerThreads = _originalMaxWorkerSize;
                var maxCompletionThreads = _originalMaxCompletionSize;

                if (maxWorkerThreads < workerthreads || maxCompletionThreads < completionThreads)
                {
                    ThreadPool.SetMaxThreads(Math.Max(maxWorkerThreads, workerthreads),
                        Math.Max(maxCompletionThreads, completionThreads));
                }
                ThreadPool.SetMinThreads(workerthreads, completionThreads);
            }
        }
        
        public void Dispose()
        {
            if (_threadPoolChanged)
            {
                ThreadPool.SetMinThreads(_originalMinWorkerSize, _originalMinCompletionSize);
                ThreadPool.SetMaxThreads(_originalMaxWorkerSize, _originalMaxCompletionSize);
            }
        }
    }
}
