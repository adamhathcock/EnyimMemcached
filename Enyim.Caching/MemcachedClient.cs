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
    public partial class MemcachedClient : IMemcachedClient, IMemcachedResultsClient
    {
        /// <summary>
        /// Represents a value which indicates that an item should never expire.
        /// </summary>
        public static readonly TimeSpan Infinite = TimeSpan.Zero;
        //internal static readonly MemcachedClientSection DefaultSettings = ConfigurationManager.GetSection("enyim.com/memcached") as MemcachedClientSection;
        private ILogger _loggger;

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

        public MemcachedClient(ILoggerFactory logggerFactory)
        {
            _loggger = logggerFactory.CreateLogger<MemcachedClient>();
            IMemcachedClientConfiguration configuration = new MemcachedClientConfiguration(_loggger); 
            configuration.SocketPool.MinPoolSize = 20;
            configuration.SocketPool.MaxPoolSize = 1000;
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

        /// <summary>
        /// Retrieves the specified item from the cache.
        /// </summary>
        /// <param name="key">The identifier for the item to retrieve.</param>
        /// <returns>The retrieved item, or <value>null</value> if the key was not found.</returns>
        [Obsolete]
        public object Get(string key)
        {
            object tmp;

            return this.TryGet(key, out tmp) ? tmp : null;
        }

        /// <summary>
        /// Retrieves the specified item from the cache.
        /// </summary>
        /// <param name="key">The identifier for the item to retrieve.</param>
        /// <returns>The retrieved item, or <value>default(T)</value> if the key was not found.</returns>
        [Obsolete]
        public T Get<T>(string key)
        {
            object tmp;

            return TryGet(key, out tmp) ? (T)tmp : default(T);
        }

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
                        return result;
                    }
                    else {
                        var tempResult = this.transcoder.Deserialize(command.Result);
                        if (tempResult != null)
                        {
                            result.Success = true;
                            result.Value = (T)tempResult;
                            return result;
                        }
                    }
                }
            }
            else
            {
                _loggger.LogError($"Unable to locate memcached node");
            }

            result.Success = false;
            result.Value = default(T);
            return result;
        }

        /// <summary>
        /// Tries to get an item from the cache.
        /// </summary>
        /// <param name="key">The identifier for the item to retrieve.</param>
        /// <param name="value">The retrieved item or null if not found.</param>
        /// <returns>The <value>true</value> if the item was successfully retrieved.</returns>
        [Obsolete]
        public bool TryGet(string key, out object value)
        {
            ulong cas = 0;

            return this.PerformTryGet(key, out cas, out value).Success;
        }

        [Obsolete]
        public CasResult<object> GetWithCas(string key)
        {
            return this.GetWithCas<object>(key);
        }

        [Obsolete]
        public CasResult<T> GetWithCas<T>(string key)
        {
            CasResult<object> tmp;

            return this.TryGetWithCas(key, out tmp)
                    ? new CasResult<T> { Cas = tmp.Cas, Result = (T)tmp.Result }
                    : new CasResult<T> { Cas = tmp.Cas, Result = default(T) };
        }

        [Obsolete]
        public bool TryGetWithCas(string key, out CasResult<object> value)
        {
            object tmp;
            ulong cas;

            var retval = this.PerformTryGet(key, out cas, out tmp);

            value = new CasResult<object> { Cas = cas, Result = tmp };

            return retval.Success;
        }

        protected virtual IGetOperationResult PerformTryGet(string key, out ulong cas, out object value)
        {
            var hashedKey = this.keyTransformer.Transform(key);
            var node = this.pool.Locate(hashedKey);
            var result = GetOperationResultFactory.Create();

            cas = 0;
            value = null;

            if (node != null)
            {
                var command = this.pool.OperationFactory.Get(hashedKey);
                var commandResult = node.Execute(command);

                if (commandResult.Success)
                {
                    result.Value = value = this.transcoder.Deserialize(command.Result);
                    result.Cas = cas = command.CasValue;

                    result.Pass();
                    return result;
                }
                else
                {
                    commandResult.Combine(result);
                    return result;
                }
            }

            result.Value = value;
            result.Cas = cas;

            result.Fail("Unable to locate node");
            return result;
        }


        #region [ Store                        ]

        /// <summary>
        /// Inserts an item into the cache with a cache key to reference its location.
        /// </summary>
        /// <param name="mode">Defines how the item is stored in the cache.</param>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="value">The object to be inserted into the cache.</param>
        /// <remarks>The item does not expire unless it is removed due memory pressure.</remarks>
        /// <returns>true if the item was successfully stored in the cache; false otherwise.</returns>
        public bool Store(StoreMode mode, string key, object value)
        {
            ulong tmp = 0;
            int status;

            return this.PerformStore(mode, key, value, 0, ref tmp, out status).Success;
        }

        /// <summary>
        /// Inserts an item into the cache with a cache key to reference its location.
        /// </summary>
        /// <param name="mode">Defines how the item is stored in the cache.</param>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="value">The object to be inserted into the cache.</param>
        /// <param name="validFor">The interval after the item is invalidated in the cache.</param>
        /// <returns>true if the item was successfully stored in the cache; false otherwise.</returns>
        public bool Store(StoreMode mode, string key, object value, TimeSpan validFor)
        {
            ulong tmp = 0;
            int status;

            return this.PerformStore(mode, key, value, MemcachedClient.GetExpiration(validFor), ref tmp, out status).Success;
        }

        public async Task<bool> StoreAsync(StoreMode mode, string key, object value, TimeSpan validFor)
        {
            return (await this.PerformStoreAsync(mode, key, value, MemcachedClient.GetExpiration(validFor))).Success;
        }

        /// <summary>
        /// Inserts an item into the cache with a cache key to reference its location.
        /// </summary>
        /// <param name="mode">Defines how the item is stored in the cache.</param>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="value">The object to be inserted into the cache.</param>
        /// <param name="expiresAt">The time when the item is invalidated in the cache.</param>
        /// <returns>true if the item was successfully stored in the cache; false otherwise.</returns>
        public bool Store(StoreMode mode, string key, object value, DateTime expiresAt)
        {
            ulong tmp = 0;
            int status;

            return this.PerformStore(mode, key, value, MemcachedClient.GetExpiration(expiresAt), ref tmp, out status).Success;
        }

        /// <summary>
        /// Inserts an item into the cache with a cache key to reference its location and returns its version.
        /// </summary>
        /// <param name="mode">Defines how the item is stored in the cache.</param>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="value">The object to be inserted into the cache.</param>
        /// <remarks>The item does not expire unless it is removed due memory pressure.</remarks>
        /// <returns>A CasResult object containing the version of the item and the result of the operation (true if the item was successfully stored in the cache; false otherwise).</returns>
        public CasResult<bool> Cas(StoreMode mode, string key, object value, ulong cas)
        {
            var result = this.PerformStore(mode, key, value, 0, cas);
            return new CasResult<bool> { Cas = result.Cas, Result = result.Success, StatusCode = result.StatusCode.Value };

        }

        /// <summary>
        /// Inserts an item into the cache with a cache key to reference its location and returns its version.
        /// </summary>
        /// <param name="mode">Defines how the item is stored in the cache.</param>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="value">The object to be inserted into the cache.</param>
        /// <param name="validFor">The interval after the item is invalidated in the cache.</param>
        /// <param name="cas">The cas value which must match the item's version.</param>
        /// <returns>A CasResult object containing the version of the item and the result of the operation (true if the item was successfully stored in the cache; false otherwise).</returns>
        public CasResult<bool> Cas(StoreMode mode, string key, object value, TimeSpan validFor, ulong cas)
        {
            var result = this.PerformStore(mode, key, value, MemcachedClient.GetExpiration(validFor), cas);
            return new CasResult<bool> { Cas = result.Cas, Result = result.Success, StatusCode = result.StatusCode.Value };
        }

        /// <summary>
        /// Inserts an item into the cache with a cache key to reference its location and returns its version.
        /// </summary>
        /// <param name="mode">Defines how the item is stored in the cache.</param>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="value">The object to be inserted into the cache.</param>
        /// <param name="expiresAt">The time when the item is invalidated in the cache.</param>
        /// <param name="cas">The cas value which must match the item's version.</param>
        /// <returns>A CasResult object containing the version of the item and the result of the operation (true if the item was successfully stored in the cache; false otherwise).</returns>
        public CasResult<bool> Cas(StoreMode mode, string key, object value, DateTime expiresAt, ulong cas)
        {
            var result = this.PerformStore(mode, key, value, MemcachedClient.GetExpiration(expiresAt), cas);
            return new CasResult<bool> { Cas = result.Cas, Result = result.Success, StatusCode = result.StatusCode.Value };
        }

        /// <summary>
        /// Inserts an item into the cache with a cache key to reference its location and returns its version.
        /// </summary>
        /// <param name="mode">Defines how the item is stored in the cache.</param>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="value">The object to be inserted into the cache.</param>
        /// <remarks>The item does not expire unless it is removed due memory pressure. The text protocol does not support this operation, you need to Store then GetWithCas.</remarks>
        /// <returns>A CasResult object containing the version of the item and the result of the operation (true if the item was successfully stored in the cache; false otherwise).</returns>
        public CasResult<bool> Cas(StoreMode mode, string key, object value)
        {
            var result = this.PerformStore(mode, key, value, 0, 0);
            return new CasResult<bool> { Cas = result.Cas, Result = result.Success, StatusCode = result.StatusCode.Value };
        }

        private IStoreOperationResult PerformStore(StoreMode mode, string key, object value, uint expires, ulong cas)
        {
            ulong tmp = cas;
            int status;

            var retval = this.PerformStore(mode, key, value, expires, ref tmp, out status);
            retval.StatusCode = status;

            if (retval.Success)
            {
                retval.Cas = tmp;
            }
            return retval;
        }

        protected virtual IStoreOperationResult PerformStore(StoreMode mode, string key, object value, uint expires, ref ulong cas, out int statusCode)
        {
            //var hashedKey = this.keyTransformer.Transform(key);
            var node = this.pool.Locate(key);
            var result = StoreOperationResultFactory.Create();           

            statusCode = -1;
            

            if (value == null)
            {
                result.Fail("value is null");
                return result;
            }

            if (node != null)
            {
                CacheItem item;

                try { item = this.transcoder.Serialize(value); }
                catch (Exception e)
                {
                    _loggger.LogError("PerformStore", e);

                    result.Fail("PerformStore failed", e);
                    return result;
                }

                var command = this.pool.OperationFactory.Store(mode, key, item, expires, cas);
                var commandResult = node.Execute(command);

                result.Cas = cas = command.CasValue;
                result.StatusCode = statusCode = command.StatusCode;

                if (commandResult.Success)
                {
                    result.Pass();
                    return result;
                }

                commandResult.Combine(result);
                return result;
            }

            //if (this.performanceMonitor != null) this.performanceMonitor.Store(mode, 1, false);

            result.Fail("Unable to locate node");
            return result;
        }

        protected async virtual Task<IStoreOperationResult> PerformStoreAsync(StoreMode mode, string key, object value, uint expires)
        {
            var node = this.pool.Locate(key);
            var result = StoreOperationResultFactory.Create();

            int statusCode = -1;
            ulong cas = 0;
            if (value == null)
            {
                result.Fail("value is null");
                return result;
            }

            if (node != null)
            {
                CacheItem item;

                try { item = this.transcoder.Serialize(value); }
                catch (Exception e)
                {
                    _loggger.LogError("PerformStoreAsync", e);

                    result.Fail("PerformStore failed", e);
                    return result;
                }

                var command = this.pool.OperationFactory.Store(mode, key, item, expires, cas);
                var commandResult = await node.ExecuteAsync(command);

                result.Cas = cas = command.CasValue;
                result.StatusCode = statusCode = command.StatusCode;

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

        #endregion
        #region [ Mutate                       ]

        #region [ Increment                    ]

        /// <summary>
        /// Increments the value of the specified key by the given amount. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to increase the item.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public ulong Increment(string key, ulong defaultValue, ulong delta)
        {
            return this.PerformMutate(MutationMode.Increment, key, defaultValue, delta, 0).Value;
        }

        /// <summary>
        /// Increments the value of the specified key by the given amount. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to increase the item.</param>
        /// <param name="validFor">The interval after the item is invalidated in the cache.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public ulong Increment(string key, ulong defaultValue, ulong delta, TimeSpan validFor)
        {
            return this.PerformMutate(MutationMode.Increment, key, defaultValue, delta, MemcachedClient.GetExpiration(validFor)).Value;
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
        public ulong Increment(string key, ulong defaultValue, ulong delta, DateTime expiresAt)
        {
            return this.PerformMutate(MutationMode.Increment, key, defaultValue, delta, MemcachedClient.GetExpiration(expiresAt)).Value;
        }

        /// <summary>
        /// Increments the value of the specified key by the given amount, but only if the item's version matches the CAS value provided. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to increase the item.</param>
        /// <param name="cas">The cas value which must match the item's version.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public CasResult<ulong> Increment(string key, ulong defaultValue, ulong delta, ulong cas)
        {
            var result = this.CasMutate(MutationMode.Increment, key, defaultValue, delta, 0, cas);
            return new CasResult<ulong> { Cas = result.Cas, Result = result.Value, StatusCode = result.StatusCode.Value };
        }

        /// <summary>
        /// Increments the value of the specified key by the given amount, but only if the item's version matches the CAS value provided. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to increase the item.</param>
        /// <param name="validFor">The interval after the item is invalidated in the cache.</param>
        /// <param name="cas">The cas value which must match the item's version.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public CasResult<ulong> Increment(string key, ulong defaultValue, ulong delta, TimeSpan validFor, ulong cas)
        {
            var result = this.CasMutate(MutationMode.Increment, key, defaultValue, delta, MemcachedClient.GetExpiration(validFor), cas);
            return new CasResult<ulong> { Cas = result.Cas, Result = result.Value, StatusCode = result.StatusCode.Value };
        }

        /// <summary>
        /// Increments the value of the specified key by the given amount, but only if the item's version matches the CAS value provided. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to increase the item.</param>
        /// <param name="expiresAt">The time when the item is invalidated in the cache.</param>
        /// <param name="cas">The cas value which must match the item's version.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public CasResult<ulong> Increment(string key, ulong defaultValue, ulong delta, DateTime expiresAt, ulong cas)
        {
            var result = this.CasMutate(MutationMode.Increment, key, defaultValue, delta, MemcachedClient.GetExpiration(expiresAt), cas);
            return new CasResult<ulong> { Cas = result.Cas, Result = result.Value, StatusCode = result.StatusCode.Value };
        }

        #endregion
        #region [ Decrement                    ]
        /// <summary>
        /// Decrements the value of the specified key by the given amount. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to decrease the item.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public ulong Decrement(string key, ulong defaultValue, ulong delta)
        {
            return this.PerformMutate(MutationMode.Decrement, key, defaultValue, delta, 0).Value;
        }

        /// <summary>
        /// Decrements the value of the specified key by the given amount. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to decrease the item.</param>
        /// <param name="validFor">The interval after the item is invalidated in the cache.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public ulong Decrement(string key, ulong defaultValue, ulong delta, TimeSpan validFor)
        {
            return this.PerformMutate(MutationMode.Decrement, key, defaultValue, delta, MemcachedClient.GetExpiration(validFor)).Value;
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
        public ulong Decrement(string key, ulong defaultValue, ulong delta, DateTime expiresAt)
        {
            return this.PerformMutate(MutationMode.Decrement, key, defaultValue, delta, MemcachedClient.GetExpiration(expiresAt)).Value;
        }

        /// <summary>
        /// Decrements the value of the specified key by the given amount, but only if the item's version matches the CAS value provided. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to decrease the item.</param>
        /// <param name="cas">The cas value which must match the item's version.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public CasResult<ulong> Decrement(string key, ulong defaultValue, ulong delta, ulong cas)
        {
            var result = this.CasMutate(MutationMode.Decrement, key, defaultValue, delta, 0, cas);
            return new CasResult<ulong> { Cas = result.Cas, Result = result.Value, StatusCode = result.StatusCode.Value };
        }

        /// <summary>
        /// Decrements the value of the specified key by the given amount, but only if the item's version matches the CAS value provided. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to decrease the item.</param>
        /// <param name="validFor">The interval after the item is invalidated in the cache.</param>
        /// <param name="cas">The cas value which must match the item's version.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public CasResult<ulong> Decrement(string key, ulong defaultValue, ulong delta, TimeSpan validFor, ulong cas)
        {
            var result = this.CasMutate(MutationMode.Decrement, key, defaultValue, delta, MemcachedClient.GetExpiration(validFor), cas);
            return new CasResult<ulong> { Cas = result.Cas, Result = result.Value, StatusCode = result.StatusCode.Value };
        }

        /// <summary>
        /// Decrements the value of the specified key by the given amount, but only if the item's version matches the CAS value provided. The operation is atomic and happens on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="defaultValue">The value which will be stored by the server if the specified item was not found.</param>
        /// <param name="delta">The amount by which the client wants to decrease the item.</param>
        /// <param name="expiresAt">The time when the item is invalidated in the cache.</param>
        /// <param name="cas">The cas value which must match the item's version.</param>
        /// <returns>The new value of the item or defaultValue if the key was not found.</returns>
        /// <remarks>If the client uses the Text protocol, the item must be inserted into the cache before it can be changed. It must be inserted as a <see cref="T:System.String"/>. Moreover the Text protocol only works with <see cref="System.UInt32"/> values, so return value -1 always indicates that the item was not found.</remarks>
        public CasResult<ulong> Decrement(string key, ulong defaultValue, ulong delta, DateTime expiresAt, ulong cas)
        {
            var result = this.CasMutate(MutationMode.Decrement, key, defaultValue, delta, MemcachedClient.GetExpiration(expiresAt), cas);
            return new CasResult<ulong> { Cas = result.Cas, Result = result.Value, StatusCode = result.StatusCode.Value };
        }

        #endregion

        private IMutateOperationResult PerformMutate(MutationMode mode, string key, ulong defaultValue, ulong delta, uint expires)
        {
            ulong tmp = 0;

            return PerformMutate(mode, key, defaultValue, delta, expires, ref tmp);
        }

        private IMutateOperationResult CasMutate(MutationMode mode, string key, ulong defaultValue, ulong delta, uint expires, ulong cas)
        {
            var tmp = cas;
            var retval = PerformMutate(mode, key, defaultValue, delta, expires, ref tmp);
            retval.Cas = tmp;
            return retval;
        }

        protected virtual IMutateOperationResult PerformMutate(MutationMode mode, string key, ulong defaultValue, ulong delta, uint expires, ref ulong cas)
        {
            var hashedKey = this.keyTransformer.Transform(key);
            var node = this.pool.Locate(hashedKey);
            var result = MutateOperationResultFactory.Create();

            if (node != null)
            {
                var command = this.pool.OperationFactory.Mutate(mode, hashedKey, defaultValue, delta, expires, cas);
                var commandResult = node.Execute(command);

                result.Cas = cas = command.CasValue;
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
        /// Appends the data to the end of the specified item's data on the server.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="data">The data to be appended to the item.</param>
        /// <returns>true if the data was successfully stored; false otherwise.</returns>
        public bool Append(string key, ArraySegment<byte> data)
        {
            ulong cas = 0;

            return this.PerformConcatenate(ConcatenationMode.Append, key, ref cas, data).Success;
        }

        /// <summary>
        /// Inserts the data before the specified item's data on the server.
        /// </summary>
        /// <returns>true if the data was successfully stored; false otherwise.</returns>
        public bool Prepend(string key, ArraySegment<byte> data)
        {
            ulong cas = 0;

            return this.PerformConcatenate(ConcatenationMode.Prepend, key, ref cas, data).Success;
        }

        /// <summary>
        /// Appends the data to the end of the specified item's data on the server, but only if the item's version matches the CAS value provided.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="cas">The cas value which must match the item's version.</param>
        /// <param name="data">The data to be prepended to the item.</param>
        /// <returns>true if the data was successfully stored; false otherwise.</returns>
        public CasResult<bool> Append(string key, ulong cas, ArraySegment<byte> data)
        {
            ulong tmp = cas;
            var success = PerformConcatenate(ConcatenationMode.Append, key, ref tmp, data);

            return new CasResult<bool> { Cas = tmp, Result = success.Success };
        }

        /// <summary>
        /// Inserts the data before the specified item's data on the server, but only if the item's version matches the CAS value provided.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="cas">The cas value which must match the item's version.</param>
        /// <param name="data">The data to be prepended to the item.</param>
        /// <returns>true if the data was successfully stored; false otherwise.</returns>
        public CasResult<bool> Prepend(string key, ulong cas, ArraySegment<byte> data)
        {
            ulong tmp = cas;
            var success = PerformConcatenate(ConcatenationMode.Prepend, key, ref tmp, data);

            return new CasResult<bool> { Cas = tmp, Result = success.Success };
        }

        protected virtual IConcatOperationResult PerformConcatenate(ConcatenationMode mode, string key, ref ulong cas, ArraySegment<byte> data)
        {
            var hashedKey = this.keyTransformer.Transform(key);
            var node = this.pool.Locate(hashedKey);
            var result = ConcatOperationResultFactory.Create();

            if (node != null)
            {
                var command = this.pool.OperationFactory.Concat(mode, hashedKey, cas, data);
                var commandResult = node.Execute(command);

                if (commandResult.Success)
                {
                    result.Cas = cas = command.CasValue;
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
        public void FlushAll()
        {
            foreach (var node in this.pool.GetWorkingNodes())
            {
                var command = this.pool.OperationFactory.Flush();

                node.Execute(command);
            }
        }

        /// <summary>
        /// Returns statistics about the servers.
        /// </summary>
        /// <returns></returns>
        public ServerStats Stats()
        {
            return this.Stats(null);
        }

        public ServerStats Stats(string type)
        {
            var results = new Dictionary<IPEndPoint, Dictionary<string, string>>();
            var handles = new List<WaitHandle>();

            foreach (var node in this.pool.GetWorkingNodes())
            {
                var cmd = this.pool.OperationFactory.Stats(type);
                var action = new Func<IOperation, IOperationResult>(node.Execute);
                var mre = new ManualResetEvent(false);

                handles.Add(mre);

                action.BeginInvoke(cmd, iar =>
                {
                    using (iar.AsyncWaitHandle)
                    {
                        action.EndInvoke(iar);

                        lock (results)
                            results[((IMemcachedNode)iar.AsyncState).EndPoint] = cmd.Result;

                        mre.Set();
                    }
                }, node);
            }

            if (handles.Count > 0)
            {
                SafeWaitAllAndDispose(handles.ToArray());
            }

            return new ServerStats(results);
        }

        /// <summary>
        /// Removes the specified item from the cache.
        /// </summary>
        /// <param name="key">The identifier for the item to delete.</param>
        /// <returns>true if the item was successfully removed from the cache; false otherwise.</returns>
        public bool Remove(string key)
        {
            return ExecuteRemove(key).Success;
        }

        //TODO: Not Implement
        public async Task<bool> RemoveAsync(string key)
        {
            return (await ExecuteRemoveAsync(key)).Success;
        }

        /// <summary>
        /// Retrieves multiple items from the cache.
        /// </summary>
        /// <param name="keys">The list of identifiers for the items to retrieve.</param>
        /// <returns>a Dictionary holding all items indexed by their key.</returns>
        public IDictionary<string, object> Get(IEnumerable<string> keys)
        {
            return PerformMultiGet<object>(keys, (mget, kvp) => this.transcoder.Deserialize(kvp.Value));
        }

        public IDictionary<string, CasResult<object>> GetWithCas(IEnumerable<string> keys)
        {
            return PerformMultiGet<CasResult<object>>(keys, (mget, kvp) => new CasResult<object>
            {
                Result = this.transcoder.Deserialize(kvp.Value),
                Cas = mget.Cas[kvp.Key]
            });
        }

        protected virtual IDictionary<string, T> PerformMultiGet<T>(IEnumerable<string> keys, Func<IMultiGetOperation, KeyValuePair<string, CacheItem>, T> collector)
        {
            // transform the keys and index them by hashed => original
            // the mget results will be mapped using this index
            var hashed = new Dictionary<string, string>();
            foreach (var key in keys) hashed[this.keyTransformer.Transform(key)] = key;

            var byServer = GroupByServer(hashed.Keys);

            var retval = new Dictionary<string, T>(hashed.Count);
            var handles = new List<WaitHandle>();

            //execute each list of keys on their respective node
            foreach (var slice in byServer)
            {
                var node = slice.Key;

                var nodeKeys = slice.Value;
                var mget = this.pool.OperationFactory.MultiGet(nodeKeys);

                // we'll use the delegate's BeginInvoke/EndInvoke to run the gets parallel
                var action = new Func<IOperation, IOperationResult>(node.Execute);
                var mre = new ManualResetEvent(false);
                handles.Add(mre);

                //execute the mgets in parallel
                action.BeginInvoke(mget, iar =>
                {
                    try
                    {
                        using (iar.AsyncWaitHandle)
                            if (action.EndInvoke(iar).Success)
                            {
                                // deserialize the items in the dictionary
                                foreach (var kvp in mget.Result)
                                {
                                    string original;
                                    if (hashed.TryGetValue(kvp.Key, out original))
                                    {
                                        var result = collector(mget, kvp);

                                        // the lock will serialize the merge,
                                        // but at least the commands were not waiting on each other
                                        lock (retval) retval[original] = result;
                                    }
                                }
                            }
                    }
                    catch (Exception e)
                    {
                        _loggger.LogError("PerformMultiGet", e);
                    }
                    finally
                    {
                        // indicate that we finished processing
                        mre.Set();
                    }
                }, nodeKeys);
            }

            // wait for all nodes to finish
            if (handles.Count > 0)
            {
                SafeWaitAllAndDispose(handles.ToArray());
            }

            return retval;
        }

        protected Dictionary<IMemcachedNode, IList<string>> GroupByServer(IEnumerable<string> keys)
        {
            var retval = new Dictionary<IMemcachedNode, IList<string>>();

            foreach (var k in keys)
            {
                var node = this.pool.Locate(k);
                if (node == null) continue;

                IList<string> list;
                if (!retval.TryGetValue(node, out list))
                    retval[node] = list = new List<string>(4);

                list.Add(k);
            }

            return retval;
        }

        /// <summary>
        /// Waits for all WaitHandles and works in both STA and MTA mode.
        /// </summary>
        /// <param name="waitHandles"></param>
        private static void SafeWaitAllAndDispose(WaitHandle[] waitHandles)
        {
            try
            {
                //Not support .NET Core
                //if (Thread.CurrentThread.GetApartmentState() == ApartmentState.MTA)
                //    WaitHandle.WaitAll(waitHandles);
                //else
                    for (var i = 0; i < waitHandles.Length; i++)
                        waitHandles[i].WaitOne();
            }
            finally
            {
                for (var i = 0; i < waitHandles.Length; i++)
                    waitHandles[i].Dispose();
            }
        }

        #region [ Expiration helper            ]

        protected const int MaxSeconds = 60 * 60 * 24 * 30;
        protected static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1);

        protected static uint GetExpiration(TimeSpan validFor)
        {
            // convert timespans to absolute dates
            // infinity
            if (validFor == TimeSpan.Zero || validFor == TimeSpan.MaxValue)
                return 0;

            uint seconds = (uint)validFor.TotalSeconds;
            if (seconds > MaxSeconds)
                return GetExpiration(DateTime.Now.Add(validFor));

            return seconds;
        }

        protected static uint GetExpiration(DateTime expiresAt)
        {
            if (expiresAt < UnixEpoch)
                throw new ArgumentOutOfRangeException("expiresAt", "expiresAt must be >= 1970/1/1");

            if (expiresAt == DateTime.MaxValue)
                return 0;

            uint retval = (uint)(expiresAt.ToUniversalTime() - UnixEpoch).TotalSeconds;

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
