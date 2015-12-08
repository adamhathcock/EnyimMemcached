using System;
using System.Net;
using System.Threading;
using Enyim.Caching;
using Enyim.Caching.Configuration;
using Enyim.Caching.Memcached;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace MemcachedTest
{
	public abstract class MemcachedClientTest
	{
		private static readonly Enyim.Caching.ILog log = Enyim.Caching.LogManager.GetLogger(typeof(MemcachedClientTest));
		public const string TestObjectKey = "Hello_World";

		protected abstract MemcachedClient GetClient();
        
		public class TestData
		{
			public TestData() { }

			public string FieldA;
			public string FieldB;
			public int FieldC;
			public bool FieldD;
		}

		/// <summary>
		///A test for Store (StoreMode, string, byte[], int, int)
		///</summary>
		[Fact]
		public void StoreObjectTest()
		{
			TestData td = new TestData();
			td.FieldA = "Hello";
			td.FieldB = "World";
			td.FieldC = 19810619;
			td.FieldD = true;

			using (MemcachedClient client = GetClient())
			{
				Assert.True(client.Store(StoreMode.Set, TestObjectKey, td));
			}
		}

		//[Fact]
		//public void GetObjectTest()
		//{
		//	TestData td = new TestData();
		//	td.FieldA = "Hello";
		//	td.FieldB = "World";
		//	td.FieldC = 19810619;
		//	td.FieldD = true;

		//	using (MemcachedClient client = GetClient())
		//	{
		//		Assert.True(client.Store(StoreMode.Set, TestObjectKey, td), "Initialization failed.");

		//		TestData td2 = client.Get<TestData>(TestObjectKey);

		//		Assert.NotNull(td2);
		//		Assert.Equal(td2.FieldA, "Hello");
		//		Assert.Equal(td2.FieldB, "World");
		//		Assert.Equal(td2.FieldC, 19810619);
		//		Assert.Equal(td2.FieldD, true);
		//		Assert.ThrowsAny<Exception>(() => client.Get((string)null));
		//		Assert.ThrowsAny<Exception>(() => client.Get(String.Empty));
		//	}
		//}

		[Fact]
		public async Task DeleteObjectTest()
		{
			using (MemcachedClient client = GetClient())
			{
				TestData td = new TestData();
				Assert.True(client.Store(StoreMode.Set, TestObjectKey, td), "Initialization failed.");

				Assert.True(client.Remove(TestObjectKey), "Remove failed.");
				Assert.Null((await client.GetAsync<string>(TestObjectKey)).Value);
			}
		}

		[Fact]
		public async Task StoreStringTest()
		{
			using (MemcachedClient client = GetClient())
			{
				Assert.True(client.Store(StoreMode.Set, "TestString", "Hello world!"), "StoreString failed.");
                
                Assert.Equal("Hello world!", (await client.GetAsync<string>("TestString")).Value);
			}
		}

		[Fact]
		public async Task StoreNullTest()
		{
			using (MemcachedClient client = GetClient())
			{
				Assert.True(client.Store(StoreMode.Set, "TestNull", null), "StoreNull failed.");
                
                Assert.Null((await client.GetAsync<string>("TestNull")).Value);
			}
		}

		[Fact]
		public async Task StoreLongTest()
		{
			using (MemcachedClient client = GetClient())
			{
				Assert.True(client.Store(StoreMode.Set, "TestLong", 65432123456L), "StoreLong failed.");

				Assert.Equal(65432123456L, (await client.GetAsync<long>("TestLong")).Value);
            }
		}

		[Fact]
		public async Task StoreArrayTest()
		{
			byte[] bigBuffer = new byte[200 * 1024];

			for (int i = 0; i < bigBuffer.Length / 256; i++)
			{
				for (int j = 0; j < 256; j++)
				{
					bigBuffer[i * 256 + j] = (byte)j;
				}
			}

			using (MemcachedClient client = GetClient())
			{
				Assert.True(client.Store(StoreMode.Set, "BigBuffer", bigBuffer), "StoreArray failed");

			    byte[] bigBuffer2 = (await client.GetAsync<byte[]>("BigBuffer")).Value;

                for (int i = 0; i < bigBuffer.Length / 256; i++)
				{
					for (int j = 0; j < 256; j++)
					{
						if (bigBuffer2[i * 256 + j] != (byte)j)
						{
							Assert.Equal(j, bigBuffer[i * 256 + j]);
							break;
						}
					}
				}
			}
		}

		[Fact]
		public async Task ExpirationTestTimeSpan()
		{
			using (MemcachedClient client = GetClient())
			{
				Assert.True(client.Store(StoreMode.Set, "ExpirationTest:TimeSpan", "ExpirationTest:TimeSpan", new TimeSpan(0, 0, 5)), "Expires:Timespan failed");
				Assert.Equal("ExpirationTest:TimeSpan", (await client.GetAsync<string>("ExpirationTest:TimeSpan")).Value);

				Thread.Sleep(8000);
				Assert.Null((await client.GetAsync<string>("ExpirationTest:TimeSpan")).Value);
			}
		}

		[Fact]
		public async Task ExpirationTestDateTime()
		{
			using (MemcachedClient client = GetClient())
			{
				DateTime expiresAt = DateTime.Now.AddSeconds(5);

				Assert.True(client.Store(StoreMode.Set, "Expires:DateTime", "Expires:DateTime", expiresAt), "Expires:DateTime failed");
				Assert.Equal("Expires:DateTime", (await client.GetAsync<string>("ExpirationTest:DateTime")).Value);

				Thread.Sleep(8000);

				Assert.Null((await client.GetAsync<string>("ExpirationTest:DateTime")).Value);
			}
		}

		[Fact]
		public async Task AddSetReplaceTest()
		{
			using (MemcachedClient client = GetClient())
			{
				log.Debug("Cache should be empty.");

				Assert.True(client.Store(StoreMode.Set, "VALUE", "1"), "Initialization failed");

				log.Debug("Setting VALUE to 1.");

				Assert.Equal("1", (await client.GetAsync<string>("VALUE")).Value);

				log.Debug("Adding VALUE; this should return false.");
				Assert.False(client.Store(StoreMode.Add, "VALUE", "2"), "Add should have failed");

				log.Debug("Checking if VALUE is still '1'.");
                Assert.Equal("1", (await client.GetAsync<string>("VALUE")).Value);

                log.Debug("Replacing VALUE; this should return true.");
				Assert.True(client.Store(StoreMode.Replace, "VALUE", "4"), "Replace failed");

				log.Debug("Checking if VALUE is '4' so it got replaced.");
                Assert.Equal("4", (await client.GetAsync<string>("VALUE")).Value);

                log.Debug("Removing VALUE.");
				Assert.True(client.Remove("VALUE"), "Remove failed");

				log.Debug("Replacing VALUE; this should return false.");
				Assert.False(client.Store(StoreMode.Replace, "VALUE", "8"), "Replace should not have succeeded");

				log.Debug("Checking if VALUE is 'null' so it was not replaced.");
				Assert.Null((await client.GetAsync<string>("VALUE")).Value);

                log.Debug("Adding VALUE; this should return true.");
				Assert.True(client.Store(StoreMode.Add, "VALUE", "16"), "Item should have been Added");

				log.Debug("Checking if VALUE is '16' so it was added.");
                Assert.Equal("16", (await client.GetAsync<string>("VALUE")).Value);

                log.Debug("Passed AddSetReplaceTest.");
			}
		}

		class NonSerializableObject
		{
			public string Value;
		}

		//[Fact]
		public void NonSerializableTest()
		{
			using (MemcachedClient client = GetClient())
			{
				Assert.False(client.Store(StoreMode.Set, "VALUE", new NonSerializableObject()), "Storing a non serializable object should have failed");
			}
		}

		private string[] keyParts = { "multi", "get", "test", "key", "parts", "test", "values" };

		protected string MakeRandomKey(int partCount)
		{
			var sb = new StringBuilder();
			var rnd = new Random();

			for (var i = 0; i < partCount; i++)
			{
				sb.Append(keyParts[rnd.Next(keyParts.Length)]).Append(":");
			}

			sb.Length--;

			return sb.ToString();
		}

		[Fact]
		public virtual void MultiGetTest()
		{
			var prefix = new Random().Next(300) + ":";
			// note, this test will fail, if memcached version is < 1.2.4
			using (var client = GetClient())
			{
				var keys = new List<string>();

				for (int i = 0; i < 1000; i++)
				{
					string k = prefix + "_Hello_Multi_Get_" + i;
					keys.Add(k);

					Assert.True(client.Store(StoreMode.Set, k, i), "Store of " + k + " failed");
				}

				//Thread.Sleep(5000);

				//for (var i = 0; i < 100; i++)
				//{
				//    Assert.Equal(client.Get(keys[i]), i, "Store of " + keys[i] + " failed");
				//}

				IDictionary<string, object> retvals = client.Get(keys);

				object value;

				for (int i = 0; i < keys.Count; i++)
				{
					string key = keys[i];

					if (!retvals.TryGetValue(key, out value))
						Console.WriteLine("missing key: " + key);
				}

				Assert.Equal(keys.Count, retvals.Count);

				for (int i = 0; i < keys.Count; i++)
				{
					string key = keys[i];

					Assert.True(retvals.TryGetValue(key, out value), "missing key: " + key);
					Assert.Equal(value, i);
				}
			}
		}

		[Fact]
		public virtual void MultiGetWithCasTest()
		{
			var prefix = new Random().Next(300) + ":";
			// note, this test will fail, if memcached version is < 1.2.4
			using (var client = GetClient())
			{
				var keys = new List<string>();

				for (int i = 0; i < 1000; i++)
				{
					string k = prefix + "_Cas_Multi_Get_" + i;
					keys.Add(k);

					Assert.True(client.Store(StoreMode.Set, k, i), "Store of " + k + " failed");
				}

				var retvals = client.GetWithCas(keys);

				CasResult<object> value;

				for (int i = 0; i < keys.Count; i++)
				{
					string key = keys[i];

					if (!retvals.TryGetValue(key, out value))
						Console.WriteLine("missing key: " + key);
				}

				Assert.Equal(keys.Count, retvals.Count);

				for (int i = 0; i < keys.Count; i++)
				{
					string key = keys[i];

					Assert.True(retvals.TryGetValue(key, out value), "missing key: " + key);
					Assert.Equal(value.Result, i);
					Assert.NotEqual(value.Cas, 0UL);
				}
			}
		}

		[Fact]
		public async Task FlushTest()
		{
			using (MemcachedClient client = GetClient())
			{
				Assert.True(client.Store(StoreMode.Set, "qwer", "1"), "Initialization failed");
				Assert.True(client.Store(StoreMode.Set, "tyui", "1"), "Initialization failed");
				Assert.True(client.Store(StoreMode.Set, "polk", "1"), "Initialization failed");
				Assert.True(client.Store(StoreMode.Set, "mnbv", "1"), "Initialization failed");
				Assert.True(client.Store(StoreMode.Set, "zxcv", "1"), "Initialization failed");
				Assert.True(client.Store(StoreMode.Set, "gfsd", "1"), "Initialization failed");
                
                Assert.Equal("1", (await client.GetAsync<string>("mnbv")).Value);

                client.FlushAll();
                
                Assert.Null((await client.GetAsync<string>("qwer")).Value);
                Assert.Null((await client.GetAsync<string>("tyui")).Value);
                Assert.Null((await client.GetAsync<string>("polk")).Value);
                Assert.Null((await client.GetAsync<string>("mnbv")).Value);
                Assert.Null((await client.GetAsync<string>("zxcv")).Value);
                Assert.Null((await client.GetAsync<string>("gfsd")).Value);
			}
		}

		[Fact]
		public void IncrementLongTest()
		{
			var initialValue = 56UL * (ulong)System.Math.Pow(10, 11) + 1234;

			using (MemcachedClient client = GetClient())
			{
				Assert.Equal(initialValue, client.Increment("VALUE", initialValue, 2UL));
				Assert.Equal(initialValue + 24, client.Increment("VALUE", 10UL, 24UL));
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
