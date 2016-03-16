using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Enyim.Caching.Configuration;
using Enyim.Caching.Memcached.Results;
using Enyim.Caching.Memcached;
using Xunit;

namespace Enyim.Caching.Tests
{
	public abstract class MemcachedClientTestsBase
	{

		protected MemcachedClient _Client;
        
		public MemcachedClientTestsBase()
		{
			var config = new MemcachedClientConfiguration();
			config.AddServer("127.0.0.1", 11211);

			_Client = new MemcachedClient(config);
		}

		protected string GetUniqueKey(string prefix = null)
		{
			return (!string.IsNullOrEmpty(prefix) ? prefix + "_" : "") +
				"unit_test_" + DateTime.Now.Ticks;
		}

		protected IEnumerable<string> GetUniqueKeys(string prefix = null, int max = 5)
		{

			var keys = new List<string>(max);
			for (int i = 0; i < max; i++)
			{
				keys.Add(GetUniqueKey(prefix));
			}

			return keys;
		}

		protected string GetRandomString()
		{
			var rand = new Random((int)DateTime.Now.Ticks).Next();
			return "unit_test_value_" + rand;
		}

		protected async Task<IStoreOperationResult> Store(StoreMode mode = StoreMode.Set, string key = null, object value = null)
		{
			if (string.IsNullOrEmpty(key))
			{
				key = GetUniqueKey("store");
			}

			if (value == null)
			{
				value = GetRandomString();
			}
			return await _Client.StoreAsync(mode, key, value).ConfigureAwait(false);
		}

		protected void StoreAssertPass(IStoreOperationResult result)
		{
			Assert.True(result.Success, "Success was false");
			Assert.True(result.Cas > 0, "Cas value was 0");
			Assert.True(result.StatusCode == 0, "StatusCode was not 0");
		}

		protected void StoreAssertFail(IStoreOperationResult result)
		{
			Assert.False(result.Success, "Success was true");
			Assert.True(result.Cas == 0, "Cas value was not 0");
			Assert.True(result.StatusCode > 0, "StatusCode not greater than 0");
			Assert.Null(result.InnerResult);
		}

		protected void GetAssertPass<T>(IGetOperationResult<T> result, T expectedValue)
		{
			Assert.True(result.Success, "Success was false");
			Assert.True(result.Cas > 0, "Cas value was 0");
			Assert.True((result.StatusCode ?? 0) == 0, "StatusCode was neither 0 nor null");
			Assert.Equal(result.Value, expectedValue);
		}

		protected void GetAssertFail<T>(IGetOperationResult<T> result)
        {
            Assert.False(result.Success, "Success was true");
            Assert.True(result.Cas == 0, "Cas value was not 0");
            Assert.True(result.StatusCode > 0, "StatusCode not greater than 0");
            Assert.False(result.HasValue, "HasValue was true");
			Assert.Null(result.Value);
		}

		protected void MutateAssertPass(IMutateOperationResult result, ulong expectedValue)
        {
            Assert.True(result.Success, "Success was false");
            Assert.Equal(result.Value, expectedValue);
            Assert.True(result.Cas > 0, "Cas value was 0");
            Assert.True((result.StatusCode ?? 0) == 0, "StatusCode was neither 0 nor null");
        }

		protected void MutateAssertFail(IMutateOperationResult result)
        {
            Assert.False(result.Success, "Success was true");
            Assert.True(result.Cas > 0, "Cas value was 0");
            Assert.True((result.StatusCode ?? 1) != 0, "StatusCode was neither 0 nor null");
        }

		protected void ConcatAssertPass(IConcatOperationResult result)
        {
            Assert.True(result.Success, "Success was false");
            Assert.True(result.Cas > 0, "Cas value was 0");
            Assert.True(result.StatusCode == 0, "Cas value was not 0");
        }

		protected void ConcatAssertFail(IConcatOperationResult result)
        {
            Assert.False(result.Success, "Success was true");
            Assert.True(result.Cas == 0, "Cas value was not 0");
            Assert.True((result.StatusCode ?? 1 ) > 0, "StatusCode not greater than 0");
        }		
	}
}

#region [ License information          ]
/* ************************************************************
 * 
 *    @author Couchbase <info@couchbase.com>
 *    @copyright 2012 Couchbase, Inc.
 *    @copyright 2012 Attila Kiskó, enyim.com
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