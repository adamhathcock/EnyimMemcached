using System;
using Microsoft.Extensions.Logging;

namespace Enyim.Caching
{
    public class EnyimLogFactory : ILogFactory
    {
        private readonly ILoggerFactory loggerFactory;

        public EnyimLogFactory(ILoggerFactory loggerFactory)
        {
            this.loggerFactory = loggerFactory;
        }

        public ILog GetLogger(string name)
        {
            return new EnyimLog(loggerFactory.CreateLogger(name));
        }

        public ILog GetLogger(Type type)
        {
            return new EnyimLog(loggerFactory.CreateLogger(type.FullName));
        }

        public ILog GetLogger<T>()
        {
            return new EnyimLog(loggerFactory.CreateLogger<T>());
        }
    }
}