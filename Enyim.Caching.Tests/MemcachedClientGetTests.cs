using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Enyim.Caching.Memcached.Results.StatusCodes;
using Xunit;

namespace Enyim.Caching.Tests
{
	public class MemcachedClientGetTests : MemcachedClientTestsBase
	{

		[Fact]
		public async Task When_Getting_Existing_Item_Value_Is_Not_Null_And_Result_Is_Successful()
		{
			var key = GetUniqueKey("get");
			var value = GetRandomString();
			var storeResult = await Store(key: key, value: value);
			StoreAssertPass(storeResult);

            var getResult = await _Client.GetAsync<string>(key);
            GetAssertPass(getResult, value);
		}

		[Fact]
		public async Task When_Getting_Item_For_Invalid_Key_HasValue_Is_False_And_Result_Is_Not_Successful()
		{
			var key = GetUniqueKey("get");

            var getResult = await _Client.GetAsync<string>(key);
            Assert.Equal(getResult.StatusCode, (int)StatusCodeEnums.NotFound);
			GetAssertFail(getResult);
		}

		[Fact]
		public async Task When_Generic_Getting_Existing_Item_Value_Is_Not_Null_And_Result_Is_Successful()
		{
			var key = GetUniqueKey("get");
			var value = GetRandomString();
			var storeResult = await Store(key: key, value: value);
			StoreAssertPass(storeResult);

            var getResult = await _Client.GetAsync<string>(key);
            Assert.True(getResult.Success, "Success was false");
			Assert.True(getResult.Cas > 0, "Cas value was 0");
			Assert.True((getResult.StatusCode ?? 0) == 0, "StatusCode was neither 0 nor null");
			Assert.Equal(getResult.Value, value);
		}
        
		[Fact]
		public async Task When_Getting_Byte_Result_Is_Successful()
		{
			var key = GetUniqueKey("Get");
			const byte expectedValue = 1;
            await Store(key: key, value: expectedValue);
            var getResult = await _Client.GetAsync<byte>(key);
            GetAssertPass(getResult, expectedValue);
		}

		[Fact]
		public async Task When_Getting_SByte_Result_Is_Successful()
		{
			var key = GetUniqueKey("Get");
			const sbyte expectedValue = 1;
            await Store(key: key, value: expectedValue);
            var getResult = await _Client.GetAsync<sbyte>(key);
            GetAssertPass(getResult, expectedValue);
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