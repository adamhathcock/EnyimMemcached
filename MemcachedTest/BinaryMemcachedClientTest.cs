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
        protected override MemcachedClient GetClient()
        {
            var config = new MemcachedClientConfiguration();
            config.AddServer("127.0.0.1", 11211);
            MemcachedClient client = new MemcachedClient(config);
            client.FlushAll();

            return client;
        }

        [Fact]
        public void IncrementTest()
        {
            using (MemcachedClient client = GetClient())
            {
                Assert.Equal(100, (int)client.Increment("VALUE", 100, 2));
                Assert.Equal(124, (int)client.Increment("VALUE", 10, 24));
            }
        }

        [Fact]
        public void DecrementTest()
        {
            using (MemcachedClient client = GetClient())
            {
                Assert.Equal(100, (int)client.Decrement("VALUE", 100, 2));
                Assert.Equal(76, (int)client.Decrement("VALUE", 10, 24));

                Assert.Equal(0, (int)client.Decrement("VALUE", 100, 1000));
            }
        }

        [Fact]
        public void IncrementNoDefaultTest()
        {
            using (MemcachedClient client = GetClient())
            {
                Assert.Null(client.Get("VALUE"));

                Assert.Equal(2, (int)client.Increment("VALUE", 2, 2));

                var value = client.Get("VALUE");
                Assert.Equal("2", value);
            }
        }

        [Fact]
        public virtual void CASTest()
        {
            using (MemcachedClient client = GetClient())
            {
                // store the item
                var r1 = client.Cas(StoreMode.Set, "CasItem1", "foo");

                Assert.True(r1.Result, "Initial set failed.");
                Assert.NotEqual(r1.Cas, (ulong)0);

                // get back the item and check the cas value (it should match the cas from the set)
                var r2 = client.GetWithCas<string>("CasItem1");

                Assert.Equal(r2.Result, "foo");
                Assert.Equal(r1.Cas, r2.Cas);

                var r3 = client.Cas(StoreMode.Set, "CasItem1", "bar", r1.Cas - 1);

                Assert.False(r3.Result,  "Overwriting with 'bar' should have failed.");

                var r4 = client.Cas(StoreMode.Set, "CasItem1", "baz", r2.Cas);

                Assert.True(r4.Result, "Overwriting with 'baz' should have succeeded.");

                var r5 = client.GetWithCas<string>("CasItem1");
                Assert.Equal(r5.Result, "baz");
            }
        }

        [Fact]
        public void AppendCASTest()
        {
            using (MemcachedClient client = GetClient())
            {
                // store the item
                var r1 = client.Cas(StoreMode.Set, "CasAppend", "foo");

                Assert.True(r1.Result, "Initial set failed.");
                Assert.NotEqual(r1.Cas, (ulong)0);

                var r2 = client.Append("CasAppend", r1.Cas, new System.ArraySegment<byte>(new byte[] { (byte)'l' }));

                Assert.True(r2.Result, "Append should have succeeded.");

                // get back the item and check the cas value (it should match the cas from the set)
                var r3 = client.GetWithCas<string>("CasAppend");

                Assert.Equal(r3.Result, "fool");
                Assert.Equal(r2.Cas, r3.Cas);

                var r4 = client.Append("CasAppend", r1.Cas, new System.ArraySegment<byte>(new byte[] { (byte)'l' }));
                Assert.False(r4.Result, "Append with invalid CAS should have failed.");
            }
        }

        [Fact]
        public void PrependCASTest()
        {
            using (MemcachedClient client = GetClient())
            {
                // store the item
                var r1 = client.Cas(StoreMode.Set, "CasPrepend", "ool");

                Assert.True(r1.Result, "Initial set failed.");
                Assert.NotEqual(r1.Cas, (ulong)0);

                var r2 = client.Prepend("CasPrepend", r1.Cas, new System.ArraySegment<byte>(new byte[] { (byte)'f' }));

                Assert.True(r2.Result, "Prepend should have succeeded.");

                // get back the item and check the cas value (it should match the cas from the set)
                var r3 = client.GetWithCas<string>("CasPrepend");

                Assert.Equal(r3.Result, "fool");
                Assert.Equal(r2.Cas, r3.Cas);

                var r4 = client.Prepend("CasPrepend", r1.Cas, new System.ArraySegment<byte>(new byte[] { (byte)'l' }));
                Assert.False(r4.Result, "Prepend with invalid CAS should have failed.");
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
