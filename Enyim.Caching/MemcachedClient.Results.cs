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
	public partial class MemcachedClient : IMemcachedClient, IMemcachedResultsClient
	{
		#region [ Concatenate		]

		/// <summary>
		/// Appends the data to the end of the specified item's data on the server.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="data">The data to be appended to the item.</param>
		/// <returns>true if the data was successfully stored; false otherwise.</returns>
		public IConcatOperationResult ExecuteAppend(string key, ArraySegment<byte> data)
		{
			ulong cas = 0;

			return this.PerformConcatenate(ConcatenationMode.Append, key, ref cas, data);
		}

		/// <summary>
		/// Appends the data to the end of the specified item's data on the server, but only if the item's version matches the CAS value provided.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="cas">The cas value which must match the item's version.</param>
		/// <param name="data">The data to be prepended to the item.</param>
		/// <returns>true if the data was successfully stored; false otherwise.</returns>
		public IConcatOperationResult ExecuteAppend(string key, ulong cas, ArraySegment<byte> data)
		{
			ulong tmp = cas;
			var result = PerformConcatenate(ConcatenationMode.Append, key, ref tmp, data);
			if (result.Success)
			{
				result.Cas = tmp;
			}
			return result;
		}

		/// <summary>
		/// Inserts the data before the specified item's data on the server.
		/// </summary>
		/// <returns>true if the data was successfully stored; false otherwise.</returns>
		public IConcatOperationResult ExecutePrepend(string key, ArraySegment<byte> data)
		{
			ulong cas = 0;

			return this.PerformConcatenate(ConcatenationMode.Prepend, key, ref cas, data);
		}

		/// <summary>
		/// Inserts the data before the specified item's data on the server, but only if the item's version matches the CAS value provided.
		/// </summary>
		/// <param name="key">The key used to reference the item.</param>
		/// <param name="cas">The cas value which must match the item's version.</param>
		/// <param name="data">The data to be prepended to the item.</param>
		/// <returns>true if the data was successfully stored; false otherwise.</returns>
		public IConcatOperationResult ExecutePrepend(string key, ulong cas, ArraySegment<byte> data)
		{
			ulong tmp = cas;
			var result = PerformConcatenate(ConcatenationMode.Prepend, key, ref tmp, data);

			if (result.Success)
			{
				result.Cas = tmp;
			}
			return result;
		}
		
		#endregion

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