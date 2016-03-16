using System;
using System.Linq;
using Enyim.Caching.Configuration;
using Enyim.Caching.Memcached;
using System.Collections.Generic;
using System.Threading;
using System.Net;
using System.Diagnostics;
using Enyim.Caching.Memcached.Results;
using Enyim.Caching.Memcached.Results.Factories;
using Enyim.Caching.Memcached.Results.Extensions;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Enyim.Caching
{
    /// <summary>
    /// Memcached client.
    /// </summary>
    public partial class MemcachedClient : IMemcachedClient
    {
        /// <summary>
        /// Represents a value which indicates that an item should never expire.
        /// </summary>
        public static readonly TimeSpan Infinite = TimeSpan.Zero;
        //internal static readonly MemcachedClientSection DefaultSettings = ConfigurationManager.GetSection("enyim.com/memcached") as MemcachedClientSection;
        private static readonly ILog log = LogManager.GetLogger<MemcachedClient>();

        private IServerPool pool;
        private IMemcachedKeyTransformer keyTransformer;
        private ITranscoder transcoder;

        public IStoreOperationResultFactory StoreOperationResultFactory { get; set; }
        public IGetOperationResultFactory GetOperationResultFactory { get; set; }
        public IMutateOperationResultFactory MutateOperationResultFactory { get; set; }
        public IConcatOperationResultFactory ConcatOperationResultFactory { get; set; }
        public IRemoveOperationResultFactory RemoveOperationResultFactory { get; set; }
  
        protected IServerPool Pool { get { return this.pool; } }
        protected IMemcachedKeyTransformer KeyTransformer { get { return this.keyTransformer; } }
        protected ITranscoder Transcoder { get { return this.transcoder; } }

        public MemcachedClient()
            :this(new MemcachedClientConfiguration())
        {
        }

        public MemcachedClient(IMemcachedClientConfiguration configuration)
        {
            configuration.SocketPool.MaxPoolSize = 1000;
            configuration.SocketPool.MinPoolSize = 20;
            configuration.SocketPool.ConnectionTimeout = new TimeSpan(0, 0, 3);
            configuration.SocketPool.ReceiveTimeout = new TimeSpan(0, 0, 3);
            configuration.SocketPool.DeadTimeout = new TimeSpan(0, 0, 3);

            this.keyTransformer = configuration.CreateKeyTransformer() ?? new DefaultKeyTransformer();
            this.transcoder = configuration.CreateTranscoder() ?? new DefaultTranscoder();

            this.pool = configuration.CreatePool();
            this.StartPool();

            StoreOperationResultFactory = new DefaultStoreOperationResultFactory();
            GetOperationResultFactory = new DefaultGetOperationResultFactory();
            MutateOperationResultFactory = new DefaultMutateOperationResultFactory();
            ConcatOperationResultFactory = new DefaultConcatOperationResultFactory();
            RemoveOperationResultFactory = new DefaultRemoveOperationResultFactory();
        }      

        public MemcachedClient(IServerPool pool, IMemcachedKeyTransformer keyTransformer, ITranscoder transcoder)
        {
            if (pool == null) throw new ArgumentNullException("pool");
            if (keyTransformer == null) throw new ArgumentNullException("keyTransformer");
            if (transcoder == null) throw new ArgumentNullException("transcoder");

            this.keyTransformer = keyTransformer;
            this.transcoder = transcoder;

            this.pool = pool;
            this.StartPool();
        }

        private void StartPool()
        {
            this.pool.NodeFailed += (n) => { var f = this.NodeFailed; if (f != null) f(n); };
            this.pool.Start();
        }

        public event Action<IMemcachedNode> NodeFailed;

      
        public async Task<IGetOperationResult<T>> GetAsync<T>(string key)
        {
            var result = new DefaultGetOperationResultFactory<T>().Create();

            //var hashedKey = this.keyTransformer.Transform(key);
            var node = this.pool.Locate(key);

            if (node != null)
            {
                var command = this.pool.OperationFactory.Get(key);
                var commandResult = await node.ExecuteAsync(command);

                if (commandResult.Success)
                {
                    if (typeof(T).GetTypeCode() == TypeCode.Object)
                    {
                        result.Success = true;
                        result.Value = this.transcoder.Deserialize<T>(command.Result);
                        result.Cas = command.CasValue;
                        result.StatusCode = command.StatusCode;
                        return result;
                    }
                    else {
                        var tempResult = this.transcoder.Deserialize(command.Result);
                        if (tempResult != null)
                        {
                            result.Success = true;
                            result.Value = (T)tempResult;
                            result.Cas = command.CasValue;
                            result.StatusCode = command.StatusCode;
                            return result;
                        }
                    }
                }
                result.Cas = command.CasValue;
                result.StatusCode = command.StatusCode;
            }
            else
            {
                log.Error($"Unable to locate memcached node");
            }

            result.Success = false;
            result.Value = default(T);
            return result;
        }
        

        public async Task<IStoreOperationResult> StoreAsync(StoreMode mode, string key, object value, TimeSpan? validFor, ulong? cas)
        {
            return await this.PerformStoreAsync(mode, key, value, GetExpiration(validFor), cas).ConfigureAwait(false);
        }
        public async Task<IStoreOperationResult> StoreAsync(StoreMode mode, string key, object value, DateTime? validUntil, ulong? cas)
        {
            return await this.PerformStoreAsync(mode, key, value, GetExpiration(validUntil), cas).ConfigureAwait(false);
        }
        protected async Task<IStoreOperationResult> PerformStoreAsync(StoreMode mode, string key, object value, uint expires, ulong? cas)
        {
            var node = this.pool.Locate(key);
            var result = StoreOperationResultFactory.Create();
            
            //if (value == null)
            //{
            //    result.Fail("value is null");
            //    return result;
            //}

            if (node != null)
            {
                CacheItem item;

                try
                {
                    item = this.transcoder.Serialize(value);
                }
                catch (Exception e)
                {
                    log.Error("PerformStoreAsync", e);

                    result.Fail("PerformStore failed", e);
                    return result;
                }

                var command = this.pool.OperationFactory.Store(mode, key, item, expires, cas ?? 0);
                var commandResult = await node.ExecuteAsync(command);

                result.Cas = command.CasValue;
                result.StatusCode = command.StatusCode;

                if (commandResult.Success)
                {
                    result.Pass();
                    return result;
                }

                commandResult.Combine(result);
                return result;
            }

            //if (this.performanceMonitor != null) this.performanceMonitor.Store(mode, 1, false);

            result.Fail("Unable to locate memcached node");
            return result;
        }
        
        #region [ Mutate                       ]

        #region [ Increment                    ]

    
        /// <summary>
        /// Increments the value of the specified key by the given amount. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to increase the item.</param>
        /// <param name="validFor">The interval after the item is invalidated in the cache.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public Task<IMutateOperationResult> IncrementAsync(string key, ulong defaultValue, ulong delta, TimeSpan? validFor, ulong? cas)
        {
            return this.PerformMutateAsync(MutationMode.Increment, key, defaultValue, delta, MemcachedClient.GetExpiration(validFor), cas);
        }

        /// <summary>
        /// Increments the value of the specified key by the given amount. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to increase the item.</param>
        /// <param name="expiresAt">The time when the item is invalidated in the cache.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public Task<IMutateOperationResult> IncrementAsync(string key, ulong defaultValue, ulong delta, DateTime? expiresAt, ulong? cas)
        {
            return this.PerformMutateAsync(MutationMode.Increment, key, defaultValue, delta, MemcachedClient.GetExpiration(expiresAt), cas);
        }

       

        #endregion
        #region [ Decrement                    ]
   
        /// Decrements the value of the specified key by the given amount. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to decrease the item.</param>
        /// <param name="validFor">The interval after the item is invalidated in the cache.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public Task<IMutateOperationResult> DecrementAsync(string key, ulong defaultValue, ulong delta, TimeSpan? validFor, ulong? cas)
        {
            return this.PerformMutateAsync(MutationMode.Decrement, key, defaultValue, delta, MemcachedClient.GetExpiration(validFor), cas);
        }

        /// <summary>
        /// Decrements the value of the specified key by the given amount. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to decrease the item.</param>
        /// <param name="expiresAt">The time when the item is invalidated in the cache.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public Task<IMutateOperationResult> DecrementAsync(string key, ulong defaultValue, ulong delta, DateTime? expiresAt, ulong? cas)
        {
            return this.PerformMutateAsync(MutationMode.Decrement, key, defaultValue, delta, MemcachedClient.GetExpiration(expiresAt), cas);
        }

       

        #endregion

    
        protected async Task<IMutateOperationResult> PerformMutateAsync(MutationMode mode, string key, ulong defaultValue, ulong delta, uint expires, ulong? cas)
        {
            var hashedKey = this.keyTransformer.Transform(key);
            var node = this.pool.Locate(hashedKey);
            var result = MutateOperationResultFactory.Create();

            if (node != null)
            {
                var command = this.pool.OperationFactory.Mutate(mode, hashedKey, defaultValue, delta, expires, cas ?? 0);
                var commandResult = await node.ExecuteAsync(command).ConfigureAwait(false);

                result.Cas = command.CasValue;
                result.StatusCode = command.StatusCode;

                if (commandResult.Success)
                {
                    result.Value = command.Result;
                    result.Pass();
                    return result;
                }
                else
                {
                    result.InnerResult = commandResult;
                    result.Fail("Mutate operation failed, see InnerResult or StatusCode for more details");
                }

            }

            // TODO not sure about the return value when the command fails
            result.Fail("Unable to locate node");
            return result;
        }


        #endregion
        #region [ Concatenate                  ]
        /// <summary>
        /// Appends the data to the end of the specified item's data on the server, but only if the item's version matches the CAS value provided.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="cas">The cas value which must match the item's version.</param>
        /// <param name="data">The data to be prepended to the item.</param>
        /// <returns>true if the data was successfully stored; false otherwise.</returns>
        public async Task<IConcatOperationResult> AppendAsync(string key, ulong? cas, ArraySegment<byte> data)
        {
            return await PerformConcatenateAsync(ConcatenationMode.Append, key, cas, data).ConfigureAwait(false);
        }

        /// <summary>
        /// Inserts the data before the specified item's data on the server, but only if the item's version matches the CAS value provided.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="cas">The cas value which must match the item's version.</param>
        /// <param name="data">The data to be prepended to the item.</param>
        /// <returns>true if the data was successfully stored; false otherwise.</returns>
        public async Task<IConcatOperationResult> PrependAsync(string key, ulong? cas, ArraySegment<byte> data)
        {
            return await PerformConcatenateAsync(ConcatenationMode.Prepend, key, cas, data).ConfigureAwait(false);
        }

        private async Task<IConcatOperationResult> PerformConcatenateAsync(ConcatenationMode mode, string key, ulong? cas, ArraySegment<byte> data)
        {
            var hashedKey = this.keyTransformer.Transform(key);
            var node = this.pool.Locate(hashedKey);
            var result = ConcatOperationResultFactory.Create();

            if (node != null)
            {
                var command = this.pool.OperationFactory.Concat(mode, hashedKey, cas ?? 0, data);
                var commandResult = await node.ExecuteAsync(command).ConfigureAwait(false);

                if (commandResult.Success)
                {
                    result.Cas = command.CasValue;
                    result.StatusCode = command.StatusCode;
                    result.Pass();
                }
                else
                {
                    result.InnerResult = commandResult;
                    result.Fail("Concat operation failed, see InnerResult or StatusCode for details");
                }

                return result;
            }

            result.Fail("Unable to locate node");
            return result;
        }

        #endregion

        /// <summary>
        /// Removes all data from the cache. Note: this will invalidate all data on all servers in the pool.
        /// </summary>
        public async Task FlushAllAsync()
        {
            await Task.WhenAll(StartAllFlushes().ToList()).ConfigureAwait(false);
        }

        private IEnumerable<Task> StartAllFlushes()
        {
            foreach (var node in this.pool.GetWorkingNodes())
            {
                var command = this.pool.OperationFactory.Flush();

                yield return node.ExecuteAsync(command);
            }
        } 

        /// <summary>
        /// Returns statistics about the servers.
        /// </summary>
        /// <returns></returns>
        public async Task<ServerStats> StatsAsync(string type = null)
        {
            var results = new Dictionary<IPEndPoint, Dictionary<string, string>>();

            foreach (var node in this.pool.GetWorkingNodes())
            {
                var cmd = this.pool.OperationFactory.Stats(type);
                await node.ExecuteAsync(cmd);
            }

            return new ServerStats(results);
        }
        
        public async Task<IRemoveOperationResult> RemoveAsync(string key)
        {
            var node = this.pool.Locate(key);
            var result = RemoveOperationResultFactory.Create();

            if (node != null)
            {
                var command = this.pool.OperationFactory.Delete(key, 0);
                var commandResult = await node.ExecuteAsync(command);

                if (commandResult.Success)
                {
                    result.Pass();
                }
                else
                {
                    result.InnerResult = commandResult;
                    result.Fail("Failed to remove item, see InnerResult or StatusCode for details");
                }

                return result;
            }

            result.Fail("Unable to locate memcached node");
            return result;
        }

        #region [ Expiration helper            ]

        protected const int MaxSeconds = 60 * 60 * 24 * 30;
        protected static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1);

        protected static uint GetExpiration(TimeSpan? validFor)
        {
            // convert timespans to absolute dates
            // infinity
            if (!validFor.HasValue)
                return 0;

            uint seconds = (uint)validFor.Value.TotalSeconds;
            if (seconds > MaxSeconds)
                return GetExpiration(DateTime.Now.Add(validFor.Value));

            return seconds;
        }

        protected static uint GetExpiration(DateTime? expiresAt)
        {
            if (expiresAt == null)
                return 0;

            if (expiresAt < UnixEpoch)
                throw new ArgumentOutOfRangeException("expiresAt", "expiresAt must be >= 1970/1/1");


            uint retval = (uint)(expiresAt.Value.ToUniversalTime() - UnixEpoch).TotalSeconds;
            return retval;
        }
        #endregion
        #region [ IDisposable                  ]

        ~MemcachedClient()
        {
            try { ((IDisposable)this).Dispose(); }
            catch { }
        }

        void IDisposable.Dispose()
        {
            this.Dispose();
        }

        /// <summary>
        /// Releases all resources allocated by this instance
        /// </summary>
        /// <remarks>You should only call this when you are not using static instances of the client, so it can close all conections and release the sockets.</remarks>
        public void Dispose()
        {
            GC.SuppressFinalize(this);

            if (this.pool != null)
            {
                try { this.pool.Dispose(); }
                finally { this.pool = null; }
            }            
        }

        #endregion
    }
}

#region [ License information          ]
/* ************************************************************
 * 
 *    Copyright (c) 2010 Attila Kisk? enyim.com
 *    
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *    
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *    
 * ************************************************************/
#endregion
