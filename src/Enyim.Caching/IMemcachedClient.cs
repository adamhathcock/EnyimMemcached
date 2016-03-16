using System;
using Enyim.Caching.Memcached;
using System.Threading.Tasks;
using Enyim.Caching.Memcached.Results;

namespace Enyim.Caching
{
    public interface IMemcachedClient : IDisposable
	{
        Task<IGetOperationResult<T>> GetAsync<T>(string key);
        Task<IStoreOperationResult> StoreAsync(StoreMode mode, string key, object value, TimeSpan? validFor, ulong? cas);
	    Task<IStoreOperationResult> StoreAsync(StoreMode mode, string key, object value, DateTime? validUntil, ulong? cas);
        Task<IRemoveOperationResult> RemoveAsync(string key);

	    Task<IConcatOperationResult> AppendAsync(string key, ulong? cas, ArraySegment<byte> data);
	    Task<IConcatOperationResult> PrependAsync(string key, ulong? cas, ArraySegment<byte> data);

        Task<IMutateOperationResult> DecrementAsync(string key, ulong defaultValue, ulong delta, DateTime? expiresAt, ulong? cas);
        Task<IMutateOperationResult> DecrementAsync(string key, ulong defaultValue, ulong delta, TimeSpan? validFor, ulong? cas);
        Task<IMutateOperationResult> IncrementAsync(string key, ulong defaultValue, ulong delta, DateTime? expiresAt, ulong? cas);
        Task<IMutateOperationResult> IncrementAsync(string key, ulong defaultValue, ulong delta, TimeSpan? validFor, ulong? cas);
        
        Task FlushAllAsync();
        
        Task<ServerStats> StatsAsync(string type = null);


        event Action<IMemcachedNode> NodeFailed;
	}
}
