using System.Threading.Tasks;
using Enyim.Caching;
using Enyim.Caching.Configuration;
using Enyim.Caching.Memcached;
using Xunit;

namespace MemcachedTest
{
	public class TextMemcachedClientTest : MemcachedClientTest
	{
		protected override async Task<MemcachedClient> GetClient()
        {
            var config = new MemcachedClientConfiguration();
            config.AddServer("127.0.0.1", 11211);
            MemcachedClient client = new MemcachedClient(config);
            await client.FlushAllAsync();
			return client;
		}

		[Fact]
		public async Task IncrementTest()
		{
			using (MemcachedClient client = await GetClient())
			{
				Assert.True((await client.StoreAsync(StoreMode.Set, "VALUE2", "100")).Success, "Initialization failed");

				Assert.Equal(102UL, (await client.IncrementAsync("VALUE2", 0, 2)).Value);
				Assert.Equal(112UL, (await client.IncrementAsync("VALUE2", 0, 10)).Value);
			}
		}

		[Fact]
		public async Task DecrementTest()
		{
			using (MemcachedClient client = await GetClient())
			{
                await client.StoreAsync(StoreMode.Set, "VALUE", "100");

				Assert.Equal(98UL, (await client.DecrementAsync("VALUE", 0, 2)).Value);
                Assert.Equal(88UL, (await client.DecrementAsync("VALUE", 0, 10)).Value);
            }
		}

		[Fact]
		public async Task CASTest()
		{
			using (MemcachedClient client = await GetClient())
			{
				// store the item
				var r1 = await client.StoreAsync(StoreMode.Set, "CasItem1", "foo");

				Assert.True(r1.Success, "Initial set failed.");

				// get back the item and check the cas value (it should match the cas from the set)
				var r2 = await client.GetAsync<string>("CasItem1");

				Assert.Equal(r2.Value, "foo");
				Assert.NotEqual(0UL, r2.Cas);

				var r3 = await client.StoreAsync(StoreMode.Set, "CasItem1", "bar", r2.Cas - 1);

				Assert.False(r3.Success, "foo");

				var r4 = await  client.StoreAsync(StoreMode.Set, "CasItem1", "baz", r2.Cas);

				Assert.True(r4.Success, "Overwriting with 'baz' should have succeeded.");
                
                var r5 = await client.GetAsync<string>("CasItem1");
                Assert.Equal(r5.Value, "baz");
			}
		}
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
