using System;
using System.Threading.Tasks;
using Enyim.Caching.Memcached;
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

        public static async Task<IConcatOperationResult> AppendAsync(this IMemcachedClient client, string key, ArraySegment<byte> data)
        {
            return await client.AppendAsync(key, null, data).ConfigureAwait(false);
        }
        public static async Task<IConcatOperationResult> PrependAsync(this IMemcachedClient client, string key, ArraySegment<byte> data)
        {
            return await client.PrependAsync(key, null, data).ConfigureAwait(false);
        }
    }
}