#region

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Remoting.Messaging;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Documents;

#endregion

namespace SQLQueryStress
{
    internal class LoadEngine
    {
        private readonly bool _collectIoStats;
        private readonly bool _collectTimeStats;
        private readonly int _commandTimeout;

        private readonly string _connectionString;
        private readonly bool _forceDataRetrieval;
        private readonly int _iterations;
        private readonly string _paramConnectionString;
        private readonly Dictionary<string, string> _paramMappings;
        private readonly string _paramQuery;
        private readonly string _query;
        private readonly int _threads;
        private int _queryDelay;
        
        public LoadEngine(string connectionString, string query, int threads, int iterations, string paramQuery, Dictionary<string, string> paramMappings,
            string paramConnectionString, int commandTimeout, bool collectIoStats, bool collectTimeStats, bool forceDataRetrieval)
        {
            //Set the min pool size so that the pool does not have
            //to get allocated in real-time
            var builder = new SqlConnectionStringBuilder(connectionString)
            {
                MinPoolSize = threads,
                CurrentLanguage = "English"
            };

            _connectionString = builder.ConnectionString;
            _query = query;
            _threads = threads;
            _iterations = iterations;
            _paramQuery = paramQuery;
            _paramMappings = paramMappings;
            _paramConnectionString = paramConnectionString;
            _commandTimeout = commandTimeout;
            _collectIoStats = collectIoStats;
            _collectTimeStats = collectTimeStats;
            _forceDataRetrieval = forceDataRetrieval;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        public static bool ExecuteCommand(string connectionString, string sql)
        {
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand(sql, conn))
                {
                    cmd.ExecuteNonQuery();
                    return true;
                }
            }
        }

        public void StartLoad(BackgroundWorker worker, int queryDelay)
        {
            _queryDelay = queryDelay;

            StartLoad(worker);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security",
            "CA2100:Review SQL queries for security vulnerabilities")]
        private void StartLoad(BackgroundWorker worker)
        {
            var queryOutInfo = new BlockingCollection<QueryOutput>();
            var cancellationTokenSource = new CancellationTokenSource();
            var queryInputs = new List<QueryInput>();
            var taskPool = new List<Task>();

            // resize threadpool
            var threadPoolSizer = new ThreadPoolSizer(_threads);

            var useParams = false;
            var badParams = new List<string>();
            foreach (var theKey in _paramMappings.Keys)
            {
                if ((_paramMappings[theKey] == null) || (_paramMappings[theKey].Length == 0))
                {
                    badParams.Add(theKey);
                }
            }

            foreach (var theKey in badParams)
            {
                _paramMappings.Remove(theKey);
            }

            //Need some parameters?
            if (_paramMappings.Count > 0)
            {
                ParamServer.Initialize(_paramQuery, _paramConnectionString, _paramMappings);
                useParams = true;
            }

            // Clear all existing connections
            SqlConnection.ClearAllPools();

            // fill the connection pool up to the "right" size, so that threads/tasks aren't waiting to open and handshake with SQL.
            Task.WaitAll(PopulateConnectionPool(_connectionString, _threads));

            // Spin up the load threads
            for (var i = 0; i < _threads; i++)
            {
                var conn = new SqlConnection(_connectionString);

                //TODO: Figure out how to make this option work (maybe)
                //conn.FireInfoMessageEventOnUserErrors = true;

                var queryComm = new SqlCommand
                {
                    CommandTimeout = _commandTimeout,
                    Connection = conn,
                    CommandText = _query
                };

                if (useParams)
                {
                    queryComm.Parameters.AddRange(ParamServer.GetParams());
                }

                var setStatistics = (_collectIoStats ? @"SET STATISTICS IO ON;" : "") +
                                    (_collectTimeStats ? @"SET STATISTICS TIME ON;" : "");

                SqlCommand statsComm = null;
                if (setStatistics.Length > 0)
                {
                    statsComm = new SqlCommand
                    {
                        CommandTimeout = _commandTimeout,
                        Connection = conn,
                        CommandText = setStatistics
                    };
                }

                var input = new QueryInput(conn, statsComm, queryComm,
                    _iterations, _forceDataRetrieval, _queryDelay);
                queryInputs.Add(input);

                var theThread = input.StartLoadThread(queryOutInfo, cancellationTokenSource.Token);
                taskPool.Add(theThread);
            }

            // event reader task, will be cancelled by the blockingcollection being completed.
            var eventReader = new Task(() => ProcessEventQueue(worker, taskPool, queryOutInfo), CancellationToken.None,
                TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach);
            eventReader.Start();

            // bit horrible, but we wake up every second to check if we've been cancelled
            // TODO: Can this be event driven, as the event only needs to set the cancellation token
            while (!Task.WaitAll(taskPool.ToArray(), TimeSpan.FromSeconds(1)))
            {
                if (worker.CancellationPending)
                {
                    cancellationTokenSource.Cancel();
                    Task.WaitAll(taskPool.ToArray());
                }
            }

            // TODO: all this clean up should really happen finally/using blocks
            // mark the event queue as complete, which will make the eventreader exit.
            queryOutInfo.CompleteAdding();

            // wait for the event reader to finish processing
            Task.WaitAll(eventReader);

            // clean up the query input objects
            foreach (var disposable in queryInputs)
                disposable.Dispose();

            threadPoolSizer.Dispose();
            if (useParams)
                ParamServer.Uninitialize();

            // free up memory back to the system
            GC.Collect(2, GCCollectionMode.Forced);
        }

        private void ProcessEventQueue(BackgroundWorker worker, ICollection<Task> taskPool, BlockingCollection<QueryOutput> queryOutInfo)
        {
            foreach (var theOut in queryOutInfo.GetConsumingEnumerable())
            {
                var finishedThreads = taskPool.Count(task => task.IsCompleted);
                worker.ReportProgress((int) (finishedThreads / (decimal) _threads * 100), theOut);
            }
        }

        private static async Task PopulateConnectionPool(string connectionString, int poolSize)
        {
            var sqlConnOpenConnections = new List<SqlConnection>(poolSize);
            var sqlConnOpenTasks = new List<Task>(30); 
            try
            {
                for (var i = 0; i < poolSize; i++)
                {
                    var sqlConn = new SqlConnection(connectionString);
                    sqlConnOpenConnections.Add(sqlConn);
                    sqlConnOpenTasks.Add(sqlConnOpenConnections[i].OpenAsync());

                    if (sqlConnOpenTasks.Count == 30)
                    {
                        // open connections in blocks of 30, otherwise some will timeout and need retries
                        await Task.WhenAll(sqlConnOpenTasks);
                        sqlConnOpenTasks.Clear();
                    }
                }
                await Task.WhenAll(sqlConnOpenTasks);
            }
            catch (Exception e)
            {
                
            }
            try
            {
                foreach (var connection in sqlConnOpenConnections)
                {
                    if (connection?.State == ConnectionState.Open)
                        connection.Close();
                }
            }
            catch (Exception e)
            {

            }
        }

        //TODO: Monostate pattern to be investigated (class is never instantiated)
        private class ParamServer
        {
            private static int _currentRow;
            private static int _numRows;

            //The actual params that will be filled
            private static SqlParameter[] _outputParams;
            //Map the param columns to ordinals in the data table
            private static int[] _paramDtMappings;
            private static DataTable _theParams;

            public static void GetNextRow_Values(SqlParameterCollection newParam)
            {
                var rowNum = Interlocked.Increment(ref _currentRow);
                var dr = _theParams.Rows[rowNum % _numRows];

                for (var i = 0; i < _outputParams.Length; i++)
                {
                    newParam[i].Value = dr[_paramDtMappings[i]];
                }
            }

            public static SqlParameter[] GetParams()
            {
                var newParam = new SqlParameter[_outputParams.Length];

                for (var i = 0; i < _outputParams.Length; i++)
                {
                    newParam[i] = (SqlParameter) ((ICloneable) _outputParams[i]).Clone();
                }

                return newParam;
            }

            [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
            public static void Initialize(string paramQuery, string connString, Dictionary<string, string> paramMappings)
            {
                var a = new SqlDataAdapter(paramQuery, connString);
                _theParams = new DataTable();
                a.Fill(_theParams);

                _numRows = _theParams.Rows.Count;

                _outputParams = new SqlParameter[paramMappings.Keys.Count];
                _paramDtMappings = new int[paramMappings.Keys.Count];

                //Populate the array of parameters that will be cloned and filled
                //on each request
                var i = 0;
                foreach (var parameterName in paramMappings.Keys)
                {
                    _outputParams[i] = new SqlParameter {ParameterName = parameterName};
                    var paramColumn = paramMappings[parameterName];

                    //if there is a param mapped to this column
                    if (paramColumn != null)
                        _paramDtMappings[i] = _theParams.Columns[paramColumn].Ordinal;

                    i++;
                }
            }

            public static void Uninitialize()
            {
                _theParams?.Clear();
                _theParams?.Dispose();
                _theParams = null;
            }           
        }

        private class QueryInput : IDisposable
        {
            //This regex is used to find the number of logical reads
            //in the messages collection returned in the queryOutput class
            private static readonly Regex FindReads = new Regex(@"(?:Table \'\w{1,}\'. Scan count \d{1,}, logical reads )(\d{1,})", RegexOptions.Compiled);

            //This regex is used to find the CPU and elapsed time
            //in the messages collection returned in the queryOutput class
            private static readonly Regex FindTimes =
                new Regex(
                    @"(?:SQL Server Execution Times:|SQL Server parse and compile time:)(?:\s{1,}CPU time = )(\d{1,})(?: ms,\s{1,}elapsed time = )(\d{1,})",
                    RegexOptions.Compiled);

            private readonly SqlConnection _sqlConnection;
            private readonly SqlCommand _queryComm;
            private readonly SqlCommand _statsComm;
            
            private readonly Stopwatch _sw = new Stopwatch();
            private readonly bool _forceDataRetrieval;
            private readonly int _iterations;
            private readonly int _queryDelay;

            public QueryInput(SqlConnection conn, SqlCommand statsComm, SqlCommand queryComm, int iterations, bool forceDataRetrieval, int queryDelay)
            {
                _sqlConnection = conn;
                _statsComm = statsComm;
                _queryComm = queryComm;
                _iterations = iterations;
                _forceDataRetrieval = forceDataRetrieval;
                _queryDelay = queryDelay;
            }

            public void Dispose()
            {
                _queryComm?.Dispose();
                _statsComm?.Dispose();
                _sqlConnection?.Dispose();
            }

            private static void GetInfoMessages(object sender, SqlInfoMessageEventArgs args, QueryOutput outInfo)
            {   
                foreach (SqlError err in args.Errors)
                {
                    var matches = FindReads.Split(err.Message);

                    //we have a read
                    if (matches.Length > 1)
                    {
                        outInfo.LogicalReads += Convert.ToInt32(matches[1]);
                        continue;
                    }

                    matches = FindTimes.Split(err.Message);

                    //we have times
                    if (matches.Length > 1)
                    {
                        outInfo.CpuTime += Convert.ToInt32(matches[1]);
                        outInfo.ElapsedTime += Convert.ToInt32(matches[2]);
                    }
                }
            }

            public async Task StartLoadThread(BlockingCollection<QueryOutput> queryOutInfo,
                CancellationToken cancellationToken)
            {
                for (var i = 0; i < _iterations; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    Exception outException = null;
                    var outInfo = new QueryOutput();
                    SqlInfoMessageEventHandler handler = (sender, args) => GetInfoMessages(sender, args, outInfo);
                    try
                    {
                        if (_sqlConnection.State != ConnectionState.Open)
                        {
                            // to save cpu burn with lots of tasks/threads it's expected that the connection is held open
                            await _sqlConnection.OpenAsync(cancellationToken).ConfigureAwait(false);

                            // set up the statistics gathering
                            if (_statsComm != null)
                            {
                                await _statsComm.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                            }
                        }

                        if (_statsComm != null)
                            _sqlConnection.InfoMessage += handler;

                        //Params are assigned only once -- after that, their values are dynamically retrieved
                        if (_queryComm.Parameters.Count > 0)
                        {
                            ParamServer.GetNextRow_Values(_queryComm.Parameters);
                        }

                        _sw.Restart();

                        //TODO: This could be made better
                        if (_forceDataRetrieval)
                        {
                            using (
                                var reader =
                                    await _queryComm.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                            {
                                do
                                {
                                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                                    {
                                        //grab the first column to force the row down the pipe
                                        var x = reader[0];
                                    }
                                } while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false));
                            }
                        }
                        else
                        {
                            await _queryComm.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                        }

                        _sw.Stop();
                    }
                    catch (TaskCanceledException)
                    {
                        throw;
                    }
                    catch (Exception e)
                    {
                        outException = e;
                        _sw.Stop();
                        _sqlConnection.Close();
                    }
                    finally
                    {
                        if (_statsComm != null)
                            _sqlConnection.InfoMessage -= handler;
                    }

                    outInfo.E = outException;
                    outInfo.Time = _sw.Elapsed;

                    // make sure we store any result, rather than losing it if cancelled
                    queryOutInfo.Add(outInfo, CancellationToken.None);

                    var finished = i == _iterations - 1;
                    if (!finished && _queryDelay > 0)
                        await Task.Delay(_queryDelay, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        public class QueryOutput
        {
            public int CpuTime;
            public Exception E;
            public int ElapsedTime;
            public int LogicalReads;
            public TimeSpan Time;
        }
    }
}