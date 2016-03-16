using System.Threading.Tasks;
using Enyim.Caching;
using Enyim.Caching.Configuration;
using Enyim.Caching.Memcached;
using Xunit;

namespace MemcachedTest
{
    /// <summary>
    ///This is a test class for Enyim.Caching.MemcachedClient and is intended
    ///to contain all Enyim.Caching.MemcachedClient Unit Tests
    ///</summary>
    public class BinaryMemcachedClientTest : MemcachedClientTest
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
                Assert.Equal(100, (int)(await client.IncrementAsync("VALUE", 100, 2)).Value);
                Assert.Equal(124, (int)(await client.IncrementAsync("VALUE", 10, 24)).Value);
            }
        }

        [Fact]
        public async Task DecrementTest()
        {
            using (MemcachedClient client = await GetClient())
            {
                Assert.Equal(100, (int)(await client.DecrementAsync("VALUE", 100, 2)).Value);
                Assert.Equal(76, (int)(await client.DecrementAsync("VALUE", 10, 24)).Value);

                Assert.Equal(0, (int)(await client.DecrementAsync("VALUE", 100, 1000)).Value);
            }
        }

        //[Fact] doesn't pass in master anyway
        public async Task IncrementNoDefaultTest()
        {
            using (MemcachedClient client = await GetClient())
            {
                Assert.Null((await client.GetAsync<string>("VALUE")).Value);

                Assert.Equal(2, (int)(await client.IncrementAsync("VALUE", 2, 2)).Value);

                var value = await client.GetAsync<string>("VALUE");
                Assert.Equal("2", value.Value);
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
                Assert.NotEqual(r1.Cas, (ulong)0);

                // get back the item and check the cas value (it should match the cas from the set)
                var r2 = await client.GetAsync<string>("CasItem1");

                Assert.Equal(r2.Value, "foo");
                Assert.Equal(r1.Cas, r2.Cas);

                var r3 = await client.StoreAsync(StoreMode.Set, "CasItem1", "bar", r1.Cas - 1);

                Assert.False(r3.Success,  "Overwriting with 'bar' should have failed.");

                var r4 = await client.StoreAsync(StoreMode.Set, "CasItem1", "baz", r2.Cas);

                Assert.True(r4.Success, "Overwriting with 'baz' should have succeeded.");
                
                var r5 = await client.GetAsync<string>("CasItem1");
                Assert.Equal(r5.Value, "baz");
            }
        }

        [Fact]
        public async Task AppendCASTest()
        {
            using (MemcachedClient client = await GetClient())
            {
                // store the item
                var r1 = await client.StoreAsync(StoreMode.Set, "CasAppend", "foo");

                Assert.True(r1.Success, "Initial set failed.");
                Assert.NotEqual(r1.Cas, (ulong)0);

                var r2 = await client.AppendAsync("CasAppend", r1.Cas, new System.ArraySegment<byte>(new byte[] { (byte)'l' }));

                Assert.True(r2.Success, "Append should have succeeded.");

                // get back the item and check the cas value (it should match the cas from the set)
                var r3 = await client.GetAsync<string>("CasAppend");

                Assert.Equal(r3.Value, "fool");
                Assert.Equal(r2.Cas, r3.Cas);

                var r4 = await client.AppendAsync("CasAppend", r1.Cas, new System.ArraySegment<byte>(new byte[] { (byte)'l' }));
                Assert.False(r4.Success, "Append with invalid CAS should have failed.");
            }
        }

        [Fact]
        public async Task PrependCASTest()
        {
            using (MemcachedClient client = await GetClient())
            {
                // store the item
                var r1 = await client.StoreAsync(StoreMode.Set, "CasPrepend", "ool");

                Assert.True(r1.Success, "Initial set failed.");
                Assert.NotEqual(r1.Cas, (ulong)0);

                var r2 = await client.PrependAsync("CasPrepend", r1.Cas, new System.ArraySegment<byte>(new byte[] { (byte)'f' }));

                Assert.True(r2.Success, "Prepend should have succeeded.");

                // get back the item and check the cas value (it should match the cas from the set)
                var r3 = await client.GetAsync<string>("CasPrepend");

                Assert.Equal(r3.Value, "fool");
                Assert.Equal(r2.Cas, r3.Cas);

                var r4 = await client.PrependAsync("CasPrepend", r1.Cas, new System.ArraySegment<byte>(new byte[] { (byte)'l' }));
                Assert.False(r4.Success, "Prepend with invalid CAS should have failed.");
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
