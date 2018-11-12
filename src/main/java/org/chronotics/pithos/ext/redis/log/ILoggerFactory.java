package org.chronotics.pithos.ext.redis.log;

public interface ILoggerFactory {
    Logger createLogger(Class<?> classz);
}
