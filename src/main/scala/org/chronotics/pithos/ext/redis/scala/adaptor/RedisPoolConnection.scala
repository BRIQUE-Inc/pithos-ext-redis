package org.chronotics.pithos.ext.redis.scala.adaptor

import com.redis.{RedisClient, RedisClientPool}
import org.chronotics.pandora.java.exception.ExceptionUtil
import org.chronotics.pandora.java.log.LoggerFactory

object RedisPoolConnection {
  var _instance: RedisPoolConnection = null

  var lockObject = new Object()

  def getInstance(host: String, port: Int, database: Int, secret: String, timeout: Int, poolWaitTimeout: Int) = {
    lockObject.synchronized {
      if (_instance == null) {
        _instance = new RedisPoolConnection(host, port, database, secret, timeout, poolWaitTimeout)
      }
    }

    _instance
  }
}

class RedisPoolConnection(host: String, port: Int, database: Int, secret: String, timeout: Int, poolWaitTimeout: Int) {
  import com.redis.serialization.Parse.Implicits.parseByteArray

  val log = LoggerFactory.getLogger(getClass());
  var lockObj: Object = new Object()

  var redisPoolCli: RedisClientPool = new RedisClientPool(host = host, port = port,
    database = database, secret = Option(secret),
    timeout = timeout, poolWaitTimeout = poolWaitTimeout)
  def keyType(curKey: String): Any = {
    try {
      redisPoolCli.withClient { redisCli =>
        redisCli.getType(curKey)
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ex.getStackTraceString)
      }
    }
  }

  def getKey(curKey: String): Any = {
    try {
      redisPoolCli.withClient { redisCli =>
        redisCli.get[String](curKey)
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ex.getStackTraceString)
        null
      }
    }
  }

  def getKeyBytes(curKey: String): Array[Byte] = {
    try {
      redisPoolCli.withClient { redisCli =>
        redisCli.get[Array[Byte]](curKey) match {
          case Some(arr) => arr
          case None => null
        }
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ex.getStackTraceString)
        null
      }
    }
  }

  def setKeyTimeout(curKey: String, timeout: Int = -1) = {
    try {
      if (timeout > -1) {
        redisPoolCli.withClient { redisCli =>
          redisCli.expire(curKey, timeout)
        }
      }
    } catch {
      case ex: Throwable => log.error("ERR: " + ex.getStackTraceString)
    }
  }

  def setKey(curKey: String, value: Any, timeout: Int = -1) = {
    try {
      redisPoolCli.withClient { redisCli =>
        redisCli.set(curKey, value)
      }

      setKeyTimeout(curKey, timeout)
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ex.getStackTraceString)
        null
      }
    }
  }

  def checkKey(curKey: String): Boolean = {
    try {
      redisPoolCli.withClient { redisCli =>
        redisCli.exists(curKey)
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ex.getStackTraceString)
        false
      }
    }
  }

  def delKey(curKey: String) = {
    try {
      redisPoolCli.withClient { redisCli =>
        redisCli.del(curKey)
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ex.getStackTraceString)
        null
      }
    }
  }

  def delKeys(lstKey: Seq[String]) = {
    try {
      redisPoolCli.withClient { redisCli =>
        lstKey.foreach(strKey => {
          redisCli.del(strKey)
        })
      }

    } catch {
      case ex: Throwable => log.error("ERR: " + ex.getStackTraceString)
    }
  }

  def hGet(curKey: String, curField: String): Any = {
    try {
      redisPoolCli.withClient { redisCli =>
        redisCli.hget[String](curKey, curField)
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ex.getStackTraceString)
        None
      }
    }
  }

  def hGetBytes(curKey: String, curField: String): Array[Byte] = {
    try {
      redisPoolCli.withClient { redisCli =>
        redisCli.hget[Array[Byte]](curKey, curField) match {
          case Some(arr) => arr
          case None => null
        }
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ExceptionUtil.getStrackTrace(ex))
        null
      }
    }
  }

  def hGetAll(curKey: String): Map[String, String] = {
    try {
      redisPoolCli.withClient { redisCli =>
        redisCli.hgetall1[String, String](curKey) match {
          case Some(map) => map
          case None => Map.empty
        }
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ex.getStackTraceString)
        Map.empty
      }
    }
  }

  def hSet(curKey: String, curField: String, curValue: Any, timeout: Int = -1) = {
    try {
      redisPoolCli.withClient { redisCli =>
        redisCli.hset(curKey, curField, curValue)
      }

      setKeyTimeout(curKey, timeout)
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ex.getStackTraceString)
        null
      }
    }
  }

  def hDel(curKey: String, curField: String) = {
    try {
      redisPoolCli.withClient { redisCli =>
        redisCli.hdel(curKey, curField)
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ex.getStackTraceString)
        null
      }
    }
  }

  def hCheckField(curKey: String, curField: String): Boolean = {
    try {
      redisPoolCli.withClient { redisCli =>
        redisCli.hexists(curKey, curField)
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ex.getStackTraceString)
        false
      }
    }
  }

  def sCount(curKey: String): Any = {
    try {
      redisPoolCli.withClient { redisCli =>
        redisCli.scard(curKey)
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ex.getStackTraceString)
        null
      }
    }
  }

  def sGet(curKey: String): Any = {
    try {
      redisPoolCli.withClient { redisCli =>
        redisCli.smembers[String](curKey) match {
          case Some(x) => x
          case None => null
        }
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ex.getStackTraceString)
        null
      }
    }
  }

  def sPop(curKey: String, count: Int): Any = {
    try {
      redisPoolCli.withClient { redisCli =>
        redisCli.spop(curKey)
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ex.getStackTraceString)
        null
      }
    }
  }

  def sSet(curKey: String, curValue: Any, timeout: Int = -1): Any = {
    try {
      redisPoolCli.withClient { redisCli =>
        redisCli.sadd(curKey, curValue)
      }

      setKeyTimeout(curKey, timeout)
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ex.getStackTraceString)
        null
      }
    }
  }

  def keys(curKeyPattern: String): Seq[String] = {
    try {
      redisPoolCli.withClient { redisCli =>
        redisCli.keys[String](curKeyPattern) match {
          case Some(lstOptKey) => {
            var lstKey: Seq[String] = Seq.empty

            lstOptKey.foreach(optKey => {
              optKey match {
                case Some(strKey) => lstKey = lstKey :+ strKey
                case None =>
              }
            })

            lstKey
          }
          case None => null
        }
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ex.getStackTraceString)
        null
      }
    }
  }

  def zCard(strKey: String): Long = {
    try {
      redisPoolCli.withClient { redisCli =>
        redisCli.zcard(strKey) match {
          case Some(lNum) => lNum
          case None => 0
        }
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR", ex.getStackTraceString)
        0
      }
    }
  }

  def zScore(strKey: String, strMember: String): Any = {
    try {
      redisPoolCli.withClient { redisCli =>
        redisCli.zscore(strKey, strMember)
      }
    } catch {
      case ex: Throwable => log.error("ERR", ex.getStackTraceString)
    }
  }

  def zRemove(strKey: String, strMember: String): Any = {
    try {
      redisPoolCli.withClient { redisCli =>
        redisCli.zrem(strKey, strMember)
      }
    } catch {
      case ex: Throwable => log.error("ERR", ex.getStackTraceString)
    }
  }

  def zAdd(strKey: String, strMember: String, dbScore: Double): Any = {
    try {
      redisPoolCli.withClient { redisCli =>
        redisCli.zadd(strKey, dbScore, strMember)
      }
    } catch {
      case ex: Throwable => log.error("ERR", ex.getStackTraceString)
    }
  }

  def zRangeByScore(strKey: String, dbMinScore: Double, dbMaxScore: Double): List[String] = {
    var lstResult: List[String] = List.empty

    try {
      redisPoolCli.withClient { redisCli =>
        redisCli.zrangebyscore[String](strKey, dbMinScore, true, dbMaxScore, true, None, RedisClient.DESC) match {
          case Some(lst) => lstResult = lst
          case None =>
        }
      }
    } catch {
      case ex: Throwable => log.error("ERR", ex.getStackTraceString)
    }

    lstResult
  }
}
