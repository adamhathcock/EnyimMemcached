﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Enyim.Caching.Memcached;
using Enyim.Caching.Configuration;
using Enyim.Caching;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace MemcachedTest
{
	public class FailurePolicyTest
	{
		[Fact]
		public async Task TestIfCalled()
		{
			var config = new MemcachedClientConfiguration();
			config.AddServer("127.0.0.1:2244");

			config.SocketPool.FailurePolicyFactory = new FakePolicy();
			config.SocketPool.ConnectionTimeout = TimeSpan.FromSeconds(4);
			config.SocketPool.ReceiveTimeout = TimeSpan.FromSeconds(6);

			var client = new MemcachedClient(config);

			Assert.Null((await client.GetAsync<string>("a")).Value);
		}

		class FakePolicy : INodeFailurePolicy, INodeFailurePolicyFactory
		{
			bool INodeFailurePolicy.ShouldFail()
			{
				Assert.True(true);

				return true;
			}

			INodeFailurePolicy INodeFailurePolicyFactory.Create(IMemcachedNode node)
			{
				return new FakePolicy();
			}
		}

		[Fact]
		public async Task TestThrottlingFailurePolicy()
		{
			var config = new MemcachedClientConfiguration();
			config.AddServer("127.0.0.1:2244");

			config.SocketPool.FailurePolicyFactory = new ThrottlingFailurePolicyFactory(4, TimeSpan.FromMilliseconds(2000));
			config.SocketPool.ConnectionTimeout = TimeSpan.FromMilliseconds(10);
			config.SocketPool.ReceiveTimeout = TimeSpan.FromMilliseconds(10);
			config.SocketPool.MinPoolSize = 1;
			config.SocketPool.MaxPoolSize = 1;

			var client = new MemcachedClient(config);
			var canFail = false;
			var didFail = false;

			client.NodeFailed += node =>
			{
				Assert.True(canFail, "canfail");

				didFail = true;
			};

            Assert.Null((await client.GetAsync<string>("a")).Value);
            Assert.Null((await client.GetAsync<string>("a")).Value);

            canFail = true;
			Thread.Sleep(2000);

            Assert.Null((await client.GetAsync<string>("a")).Value);
            Assert.Null((await client.GetAsync<string>("a")).Value);
            Assert.Null((await client.GetAsync<string>("a")).Value);
            Assert.Null((await client.GetAsync<string>("a")).Value);

            Assert.True(didFail, "didfail");
		}
	}
}