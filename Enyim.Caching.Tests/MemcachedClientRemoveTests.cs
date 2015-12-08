using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Enyim.Caching.Tests
{
	public class MemcachedClientRemoveTests : MemcachedClientTestsBase
	{
		[Fact]
		public async Task When_Removing_A_Valid_Key_Result_Is_Successful()
		{
			var key = GetUniqueKey("remove");
			var storeResult = Store(key: key);
			StoreAssertPass(storeResult);

			var removeResult = _Client.ExecuteRemove(key);
			Assert.True(removeResult.Success, "Success was false");
			Assert.True((removeResult.StatusCode ?? 0)  == 0, "StatusCode was neither null nor 0");

            var getResult = await _Client.GetAsync<string>(key);
            GetAssertFail(getResult);
		}

		[Fact]
		public void When_Removing_An_Invalid_Key_Result_Is_Not_Successful()
		{
			var key = GetUniqueKey("remove");

			var removeResult = _Client.ExecuteRemove(key);
			Assert.False(removeResult.Success, "Success was true");
		}
	}
}
