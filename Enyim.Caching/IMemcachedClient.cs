using System;
using Enyim.Caching.Memcached;
using System.Collections.Generic;
using System.Threading.Tasks;
using Enyim.Caching.Memcached.Results;

namespace Enyim.Caching
{
    public static class IMemcachedClientExtensions
    {
        public static async Task<IStoreOperationResult> StoreAsync(this IMemcachedClient client, StoreMode mode, string key, object value, DateTime validUntil)
        {
            return await client.StoreAsync(mode, key, value, validUntil, null).ConfigureAwait(false);
        }
        public static async Task<IStoreOperationResult> StoreAsync(this IMemcachedClient client, StoreMode mode, string key, object value, TimeSpan validFor)
        {
            return await client.StoreAsync(mode, key, value, validFor, null).ConfigureAwait(false);
        }
        public static async Task<IStoreOperationResult> StoreAsync(this IMemcachedClient client, StoreMode mode, string key, object value, ulong cas)
        {
            return await client.StoreAsync(mode, key, value, (DateTime?)null, cas).ConfigureAwait(false);
        }
        public static async Task<IStoreOperationResult> StoreAsync(this IMemcachedClient client, StoreMode mode, string key, object value)
        {
            return await client.StoreAsync(mode, key, value, (DateTime?)null, null).ConfigureAwait(false);
        }

        public static async Task<IMutateOperationResult> DecrementAsync(this IMemcachedClient client, string key, ulong defaultValue, ulong delta, DateTime validUntil)
        {
            return await client.DecrementAsync(key, defaultValue, delta, validUntil, null).ConfigureAwait(false);
        }
        public static async Task<IMutateOperationResult> DecrementAsync(this IMemcachedClient client, string key, ulong defaultValue, ulong delta, TimeSpan validFor)
        {
            return await client.DecrementAsync(key, defaultValue, delta, validFor, null).ConfigureAwait(false);
        }
        public static async Task<IMutateOperationResult> DecrementAsync(this IMemcachedClient client, string key, ulong defaultValue, ulong delta, ulong cas)
        {
            return await client.DecrementAsync(key, defaultValue, delta, (DateTime?)null, cas).ConfigureAwait(false);
        }
        public static async Task<IMutateOperationResult> DecrementAsync(this IMemcachedClient client, string key, ulong defaultValue, ulong delta)
        {
            return await client.DecrementAsync(key, defaultValue, delta, (DateTime?)null, null).ConfigureAwait(false);
        }

        public static async Task<IMutateOperationResult> IncrementAsync(this IMemcachedClient client, string key, ulong defaultValue, ulong delta, DateTime validUntil)
        {
            return await client.IncrementAsync(key, defaultValue, delta, validUntil, null).ConfigureAwait(false);
        }
        public static async Task<IMutateOperationResult> IncrementAsync(this IMemcachedClient client, string key, ulong defaultValue, ulong delta, TimeSpan validFor)
        {
            return await client.IncrementAsync(key, defaultValue, delta, validFor, null).ConfigureAwait(false);
        }
        public static async Task<IMutateOperationResult> IncrementAsync(this IMemcachedClient client, string key, ulong defaultValue, ulong delta, ulong cas)
        {
            return await client.DecrementAsync(key, defaultValue, delta, (DateTime?)null, cas).ConfigureAwait(false);
        }
        public static async Task<IMutateOperationResult> IncrementAsync(this IMemcachedClient client, string key, ulong defaultValue, ulong delta)
        {
            return await client.IncrementAsync(key, defaultValue, delta, (DateTime?)null, null).ConfigureAwait(false);
        }
    }

	public interface IMemcachedClient : IDisposable
	{
        Task<IGetOperationResult<T>> GetAsync<T>(string key);
        Task<IStoreOperationResult> StoreAsync(StoreMode mode, string key, object value, TimeSpan? validFor, ulong? cas);
	    Task<IStoreOperationResult> StoreAsync(StoreMode mode, string key, object value, DateTime? validUntil, ulong? cas);
        Task<IRemoveOperationResult> RemoveAsync(string key);

        bool Append(string key, ArraySegment<byte> data);
		CasResult<bool> Append(string key, ulong cas, ArraySegment<byte> data);

		bool Prepend(string key, ArraySegment<byte> data);
		CasResult<bool> Prepend(string key, ulong cas, ArraySegment<byte> data);

        Task<IMutateOperationResult> DecrementAsync(string key, ulong defaultValue, ulong delta, DateTime? expiresAt, ulong? cas);
        Task<IMutateOperationResult> DecrementAsync(string key, ulong defaultValue, ulong delta, TimeSpan? validFor, ulong? cas);
        Task<IMutateOperationResult> IncrementAsync(string key, ulong defaultValue, ulong delta, DateTime? expiresAt, ulong? cas);
        Task<IMutateOperationResult> IncrementAsync(string key, ulong defaultValue, ulong delta, TimeSpan? validFor, ulong? cas);
        
        void FlushAll();

		ServerStats Stats();
		ServerStats Stats(string type);

		event Action<IMemcachedNode> NodeFailed;
	}
}
