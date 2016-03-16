using System;
using Microsoft.Extensions.Logging;

namespace Enyim.Caching
{
	/// <summary>
	/// Creates loggers based on the current configuration.
	/// </summary>
	/// <example>
	/// 
	/// Config file:
	/// 
	/// <configuration>
	///		<configSections>
	///			<sectionGroup name="enyim.com">
	///				<section name="log" type="Enyim.Caching.EnyimLoggerSection, Enyim.Caching" />
	///			</sectionGroup>
	///		</configSections>
	///		<enyim.com>
	///			<log factory="Enyim.Caching.Log4NetLoggerFactory, Enyim.Caching" />
	///		</enyim.com>
	/// </configuration>
	/// 
	/// Code:
	/// 
	///		LogManager.AssignFactory(new Log4NetLogFactory());
	/// 
	/// </example>
	public static class LogManager
	{
		private static ILogFactory factory = new NullLoggerFactory();

		/// <summary>
		/// Assigns a new logger factory programmatically.
		/// </summary>
		/// <param name="factory"></param>
		public static void AssignFactory(ILoggerFactory factory)
		{
			if (factory == null) throw new ArgumentNullException("factory");
			LogManager.factory = new EnyimLogFactory(factory);
		}

		/// <summary>
		/// Returns a new logger for the specified Type.
		/// </summary>
		/// <param name="type"></param>
		/// <returns></returns>
		public static ILog GetLogger(Type type)
		{
		    return factory.GetLogger(type);
		}

		/// <summary>
		/// Returns a logger with the specified name.
		/// </summary>
		/// <param name="name"></param>
		/// <returns></returns>
		public static ILog GetLogger(string name)
        {
            return factory.GetLogger(name);
        }

        public static ILog GetLogger<T>()
        {
            return factory.GetLogger<T>();
        }
    }
}

#region [ License information          ]
/* ************************************************************
 * 
 *    Copyright (c) 2010 Attila Kiskó, enyim.com
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
