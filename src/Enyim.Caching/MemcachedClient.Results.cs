using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Enyim.Caching.Memcached;
using Enyim.Caching.Memcached.Results;
using Enyim.Caching.Memcached.Results.Extensions;
using Enyim.Caching.Memcached.Results.Factories;

namespace Enyim.Caching
{
	public partial class MemcachedClient : IMemcachedClient
	{
		#region [ Remove		]

		/// <summary>
		/// Removes the specified item from the cache.
		/// </summary>
		/// <param name="key">The identifier for the item to delete.</param>
		/// <returns>true if the item was successfully removed from the cache; false otherwise.</returns>
		public IRemoveOperationResult ExecuteRemove(string key)
		{
			//var hashedKey = this.keyTransformer.Transform(key);
			var node = this.pool.Locate(key);
			var result = RemoveOperationResultFactory.Create();

			if (node != null)
			{
				var command = this.pool.OperationFactory.Delete(key, 0);
				var commandResult = node.Execute(command);

				if (commandResult.Success)
				{
					result.Pass();
				}
				else
				{
					result.InnerResult = commandResult;
					result.Fail("Failed to remove item, see InnerResult or StatusCode for details");
				}

				return result;
			}

			result.Fail("Unable to locate node");
			return result;
		}


        #endregion
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