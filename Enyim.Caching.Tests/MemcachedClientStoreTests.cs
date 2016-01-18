using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Enyim.Caching.Memcached;
using Enyim.Caching.Memcached.Results;
using Enyim.Caching.Memcached.Results.StatusCodes;
using Xunit;

namespace Enyim.Caching.Tests
{
	public class MemcachedClientStoreTests : MemcachedClientTestsBase
	{

		[Fact]
		public async Task When_Storing_Item_With_New_Key_And_StoreMode_Add_Result_Is_Successful()
		{
			var result = await Store(StoreMode.Add);
			StoreAssertPass(result);

		}

		[Fact]
		public async Task When_Storing_Item_With_Existing_Key_And_StoreMode_Add_Result_Is_Not_Successful()
		{
			var key = GetUniqueKey("store");
			var result = await Store(StoreMode.Add, key);
			StoreAssertPass(result);

			result = await Store(StoreMode.Add, key);
			StoreAssertFail(result);
		}

		[Fact]
		public async Task When_Storing_Item_With_New_Key_And_StoreMode_Replace_Result_Is_Not_Successful()
		{
			var result = await Store(StoreMode.Replace);
			Assert.Equal(result.StatusCode, (int)StatusCodeEnums.NotFound);
			StoreAssertFail(result);

		}

		[Fact]
		public async Task When_Storing_Item_With_Existing_Key_And_StoreMode_Replace_Result_Is_Successful()
		{
			var key = GetUniqueKey("store");
			var result = await Store(StoreMode.Add, key);
			StoreAssertPass(result);

			result = await Store(StoreMode.Replace, key);
			StoreAssertPass(result);
		}

		[Fact]
		public async Task When_Storing_Item_With_New_Key_And_StoreMode_Set_Result_Is_Successful()
		{
			var result = await Store(StoreMode.Set);
			StoreAssertPass(result);

		}

		[Fact]
		public async Task When_Storing_Item_With_Existing_Key_And_StoreMode_Set_Result_Is_Successful()
		{
			var key = GetUniqueKey("store");
			var result = await Store(StoreMode.Add, key);
			StoreAssertPass(result);

			result = await Store(StoreMode.Set, key);
			StoreAssertPass(result);
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
