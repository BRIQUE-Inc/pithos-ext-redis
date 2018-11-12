package org.chronotics.pithos.ext.redis.adaptor;

import org.chronotics.pithos.ext.redis.log.Logger;
import org.chronotics.pithos.ext.redis.log.LoggerFactory;
import org.chronotics.pithos.ext.redis.util.ExceptionUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.*;

public class RedisConnection {
    private Logger log = LoggerFactory.getLogger(getClass());
    private JedisPool redisPool = null;
    private Integer intTimeout = 300000;
    private static HashMap<Integer, RedisConnection> mapJedis = new HashMap<>();

    public RedisConnection(String strRedisHost, Integer intRedisPort, String strRedisPass, Integer intRedisDB, Integer intMaxPool) {
        try {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(intMaxPool);
            poolConfig.setTestOnBorrow(true);
            poolConfig.setTestOnReturn(true);
            poolConfig.setMaxIdle(5);
            poolConfig.setMinIdle(1);
            poolConfig.setTestWhileIdle(true);
            poolConfig.setNumTestsPerEvictionRun(1000);
            poolConfig.setTimeBetweenEvictionRunsMillis(intTimeout);

            Integer intHashCode = (strRedisHost + intRedisPort.toString() + strRedisPass + intRedisDB.toString())
                    .hashCode();

            redisPool = new JedisPool(poolConfig, strRedisHost, intRedisPort, intTimeout, strRedisPass, intRedisDB);

            if (mapJedis.containsKey(intHashCode)) {
                mapJedis.remove(intHashCode);
            }

            mapJedis.put(intHashCode, this);
        } catch (Exception objEx) {
        }
    }

    public static RedisConnection getInstance(String strRedisHost, Integer intRedisPort, String strRedisPass, Integer intRedisDB, Integer intMaxPool) {
        Integer intCurHashCode = (strRedisHost + intRedisPort.toString() + strRedisPass + intRedisDB.toString() + intMaxPool.toString())
                .hashCode();

        if (!mapJedis.containsKey(intCurHashCode)) {
            synchronized (RedisConnection.class) {
                if (!mapJedis.containsKey(intCurHashCode)) {
                    new RedisConnection(strRedisHost, intRedisPort, strRedisPass, intRedisDB, intMaxPool);
                }
            }
        }

        return mapJedis.get(intCurHashCode);
    }

    private synchronized Jedis getJedis() {
        return redisPool.getResource();
    }

    public void closedJedisClient(Jedis objJedisClient) {
        if (objJedisClient != null) {
            try {
                objJedisClient.close();
            } catch (Exception e) {
                log.error("ERR: " + ExceptionUtil.getStrackTrace(e));
            }
        }
    }

    public Boolean setExpire(String strKey, Integer intExpireSec) {
        Boolean bIsSet = false;

        Jedis objJedisClient = getJedis();

        if (objJedisClient != null) {
            try {
                if (intExpireSec > 0) {
                    objJedisClient.expire(strKey, intExpireSec);
                }

                bIsSet = true;
            } catch (Exception objEx) {
                throw objEx;
            } finally {
                closedJedisClient(objJedisClient);
            }
        }

        return bIsSet;
    }

    public String keyType(String strKey) {
        String strType = "";

        Jedis objJedisClient = getJedis();

        if (objJedisClient != null) {
            try {
                strType = objJedisClient.type(strKey);
            } catch (Exception objEx) {
                throw objEx;
            } finally {
                closedJedisClient(objJedisClient);
            }
        }

        return strType;
    }

    public Boolean delKey(String strKey) {
        Boolean bIsDel = false;

        Jedis objJedisClient = getJedis();

        if (objJedisClient != null) {
            try {
                objJedisClient.del(strKey);

                bIsDel = true;
            } catch (Exception objEx) {
                throw objEx;
            } finally {
                closedJedisClient(objJedisClient);
            }
        }

        return bIsDel;
    }

    public Boolean setKey(String strKey, String strValue, Integer intExpireSec) {
        Boolean bIsSet = false;

        Jedis objJedisClient = getJedis();

        if (objJedisClient != null) {
            try {
                objJedisClient.set(strKey, strValue);
                setExpire(strKey, intExpireSec);

                bIsSet = true;
            } catch (Exception objEx) {
                throw objEx;
            } finally {
                closedJedisClient(objJedisClient);
            }
        }

        return bIsSet;
    }

    public String getKey(String strKey) {
        String strValue = "";

        Jedis objJedisClient = getJedis();

        if (objJedisClient != null) {
            try {
                strValue = objJedisClient.get(strKey);
            } catch (Exception objEx) {
                throw objEx;
            } finally {
                closedJedisClient(objJedisClient);
            }
        }

        return strValue;
    }


    public Boolean checkKey(String strKey) {
        Boolean bIsExist = false;

        Jedis objJedisClient = getJedis();

        if (objJedisClient != null) {
            try {
                bIsExist = objJedisClient.exists(strKey);
            } catch (Exception objEx) {
                throw objEx;
            } finally {
                closedJedisClient(objJedisClient);
            }
        }

        return bIsExist;
    }

    public Boolean hSetField(String strKey, String strField, String strValue, Integer intTimeoutSecond) {
        Boolean bSuccess = false;

        Jedis objJedisClient = getJedis();

        if (objJedisClient != null) {
            try {
                objJedisClient.hset(strKey, strField, strValue);

                if (intTimeoutSecond > 0) {
                    objJedisClient.expire(strKey, intTimeoutSecond);
                }

                bSuccess = true;
            } catch (Exception objEx) {
                throw objEx;
            } finally {
                closedJedisClient(objJedisClient);
            }
        }

        return bSuccess;
    }

    public HashMap<String, String> hGetAll(String strKey) {
        HashMap<String, String> mapValue = new HashMap<String, String>();

        Jedis objJedisClient = getJedis();

        if (objJedisClient != null) {
            try {
                mapValue = (HashMap<String, String>) objJedisClient.hgetAll(strKey);
            } catch (Exception objEx) {
                throw objEx;
            } finally {
                closedJedisClient(objJedisClient);
            }
        }

        return mapValue;
    }

    public String hGetByField(String strKey, String strField) {
        String strValue = "";

        Jedis objJedisClient = getJedis();

        if (objJedisClient != null) {
            try {
                strValue = objJedisClient.hget(strKey, strField);
            } catch (Exception objEx) {
                throw objEx;
            } finally {
                closedJedisClient(objJedisClient);
            }
        }

        return strValue;
    }

    public Boolean hDel(String strKey, String strField) {
        Boolean bIsDeleted = false;

        Jedis objJedisClient = getJedis();

        if (objJedisClient != null) {
            try {
                Long lDel = objJedisClient.hdel(strKey, strField);
                bIsDeleted = true;
            } catch (Exception objEx) {
                throw objEx;
            } finally {
                closedJedisClient(objJedisClient);
            }
        }

        return bIsDeleted;
    }

    public Long sCount(String strKey) {
        Long lTotal = 0L;

        Jedis objJedisClient = getJedis();

        if (objJedisClient != null) {
            try {
                lTotal = objJedisClient.scard(strKey);
            } catch (Exception objEx) {
                throw objEx;
            } finally {
                closedJedisClient(objJedisClient);
            }
        }

        return lTotal;
    }

    public Set<String> sGet(String strKey) {
        Set<String> setTotal = new HashSet<>();

        Jedis objJedisClient = getJedis();

        if (objJedisClient != null) {
            try {
                setTotal = objJedisClient.smembers(strKey);
            } catch (Exception objEx) {
                throw objEx;
            } finally {
                closedJedisClient(objJedisClient);
            }
        }

        return setTotal;
    }

    public Set<String> sPop(String strKey, Long lNumItem) {
        Set<String> setTotal = new HashSet<>();

        Jedis objJedisClient = getJedis();

        if (objJedisClient != null) {
            try {
                setTotal = objJedisClient.spop(strKey, lNumItem);
            } catch (Exception objEx) {
                throw objEx;
            } finally {
                closedJedisClient(objJedisClient);
            }
        }

        return setTotal;
    }

    public Boolean sSet(String strKey, String strValue, Integer intTimeoutSecond) {
        Boolean bSuccess = false;

        Jedis objJedisClient = getJedis();

        if (objJedisClient != null) {
            try {
                objJedisClient.sadd(strKey, strValue);

                if (intTimeoutSecond > 0) {
                    objJedisClient.expire(strKey, intTimeoutSecond);
                }

                bSuccess = true;
            } catch (Exception objEx) {
                throw objEx;
            } finally {
                closedJedisClient(objJedisClient);
            }
        }

        return bSuccess;
    }

    public Long zCard(String strKey) {
        Long lTotal = 0L;

        Jedis objJedisClient = getJedis();

        if (objJedisClient != null) {
            try {
                lTotal = objJedisClient.zcard(strKey);
            } catch (Exception objEx) {
                throw objEx;
            } finally {
                closedJedisClient(objJedisClient);
            }
        }

        return lTotal;
    }

    public Double zScore(String strKey, String strMember) {
        Double dbScore = 0.0;

        Jedis objJedisClient = getJedis();

        if (objJedisClient != null) {
            try {
                dbScore = objJedisClient.zscore(strKey, strMember);
            } catch (Exception objEx) {
                throw objEx;
            } finally {
                closedJedisClient(objJedisClient);
            }
        }

        return dbScore;
    }

    public Boolean zRemove(String strKey, String strMember) {
        Boolean bIsRemoved = false;

        Jedis objJedisClient = getJedis();

        if (objJedisClient != null) {
            try {
                Long lRem = objJedisClient.zrem(strKey, strMember);
                bIsRemoved = true;
            } catch (Exception objEx) {
                throw objEx;
            } finally {
                closedJedisClient(objJedisClient);
            }
        }

        return bIsRemoved;
    }

    public Boolean zAdd(String strKey, String strMember, Double dbScore, Integer intTimeoutSecond) {
        Boolean bIsAdded = false;

        Jedis objJedisClient = getJedis();

        if (objJedisClient != null) {
            try {
                Long lAdded = objJedisClient.zadd(strKey, dbScore, strMember);

                if (intTimeoutSecond > 0) {
                    objJedisClient.expire(strKey, intTimeoutSecond);
                }

                bIsAdded = true;
            } catch (Exception objEx) {
                throw objEx;
            } finally {
                closedJedisClient(objJedisClient);
            }
        }

        return bIsAdded;
    }

    public List<String> zRevRangeByScore(String strKey, Double dbMinScore, Double dbMaxScore) {
        List<String> lstResult = new ArrayList<>();

        Jedis objJedisClient = getJedis();

        if (objJedisClient != null) {
            try {
                objJedisClient.zrevrangeByScore(strKey, dbMaxScore, dbMinScore).forEach(strFoundKey -> {
                    lstResult.add(strFoundKey);
                });
            } catch (Exception objEx) {
                throw objEx;
            } finally {
                closedJedisClient(objJedisClient);
            }
        }

        return lstResult;
    }
}
