using System;
using Microsoft.Extensions.Logging;

namespace Enyim.Caching
{
    public class EnyimLog : ILog
    {
        private readonly ILogger log;

        public EnyimLog(ILogger log)
        {
            this.log = log;
        }

        public void Debug(object message)
        {
            log.LogDebug(message?.ToString());
        }

        public void Debug(object message, Exception exception)
        {
            log.LogDebug(message + Environment.NewLine + exception);
        }

        public void DebugFormat(string format, object arg0)
        {
            log.LogDebug(format, arg0);
        }

        public void DebugFormat(string format, object arg0, object arg1)
        {
            log.LogDebug(format, arg0, arg1);
        }

        public void DebugFormat(string format, object arg0, object arg1, object arg2)
        {
            log.LogDebug(format, arg0, arg1, arg2);
        }

        public void DebugFormat(string format, params object[] args)
        {
            log.LogDebug(format, args);
        }

        public void DebugFormat(IFormatProvider provider, string format, params object[] args)
        {
            log.LogDebug(format, args);
        }

        public void Info(object message)
        {
            log.LogInformation(message?.ToString());
        }

        public void Info(object message, Exception exception)
        {
            log.LogInformation(message + Environment.NewLine + exception);
        }

        public void InfoFormat(string format, object arg0)
        {
            log.LogInformation(format, arg0);
        }

        public void InfoFormat(string format, object arg0, object arg1)
        {
            log.LogInformation(format, arg0, arg1);
        }

        public void InfoFormat(string format, object arg0, object arg1, object arg2)
        {
            log.LogInformation(format, arg0, arg1, arg2);
        }

        public void InfoFormat(string format, params object[] args)
        {
            log.LogInformation(format, args);
        }

        public void InfoFormat(IFormatProvider provider, string format, params object[] args)
        {
            log.LogInformation(format, args);
        }

        public void Warn(object message)
        {
            log.LogWarning(message?.ToString());
        }

        public void Warn(object message, Exception exception)
        {
            log.LogWarning(message + Environment.NewLine + exception);
        }

        public void WarnFormat(string format, object arg0)
        {
            log.LogWarning(format, arg0);
        }

        public void WarnFormat(string format, object arg0, object arg1)
        {
            log.LogWarning(format, arg0, arg1);
        }

        public void WarnFormat(string format, object arg0, object arg1, object arg2)
        {
            log.LogWarning(format, arg0, arg1, arg2);
        }

        public void WarnFormat(string format, params object[] args)
        {
            log.LogWarning(format, args);
        }

        public void WarnFormat(IFormatProvider provider, string format, params object[] args)
        {
            log.LogWarning(format, args);
        }

        public void Error(object message)
        {
            log.LogError(message?.ToString());
        }

        public void Error(object message, Exception exception)
        {
            log.LogError(message + Environment.NewLine + exception);
        }

        public void ErrorFormat(string format, object arg0)
        {
            log.LogError(format, arg0);
        }

        public void ErrorFormat(string format, object arg0, object arg1)
        {
            log.LogError(format, arg0, arg1);
        }

        public void ErrorFormat(string format, object arg0, object arg1, object arg2)
        {
            log.LogError(format, arg0, arg1, arg2);
        }

        public void ErrorFormat(string format, params object[] args)
        {
            log.LogError(format, args);
        }

        public void ErrorFormat(IFormatProvider provider, string format, params object[] args)
        {
            log.LogError(format, args);
        }

        public void Fatal(object message)
        {
            log.LogCritical(message?.ToString());
        }

        public void Fatal(object message, Exception exception)
        {
            log.LogCritical(message + Environment.NewLine + exception);
        }

        public void FatalFormat(string format, object arg0)
        {
            log.LogCritical(format, arg0);
        }

        public void FatalFormat(string format, object arg0, object arg1)
        {
            log.LogCritical(format, arg0, arg1);
        }

        public void FatalFormat(string format, object arg0, object arg1, object arg2)
        {
            log.LogCritical(format, arg0, arg1, arg2);
        }

        public void FatalFormat(string format, params object[] args)
        {
            log.LogCritical(format, args);
        }

        public void FatalFormat(IFormatProvider provider, string format, params object[] args)
        {
            log.LogCritical(format, args);
        }



        public bool IsDebugEnabled => log.IsEnabled(LogLevel.Debug);
        public bool IsInfoEnabled => log.IsEnabled(LogLevel.Information);
        public bool IsWarnEnabled => log.IsEnabled(LogLevel.Warning);
        public bool IsErrorEnabled => log.IsEnabled(LogLevel.Error);
        public bool IsFatalEnabled => log.IsEnabled(LogLevel.Critical);
    }
}