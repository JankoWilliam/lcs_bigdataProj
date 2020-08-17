package cn.yintech.redisUtil;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class RedisClientJava {

    private RedisClientJava(){}

    public static volatile JedisPool jedisPool = null;

    public static JedisPool getJedisPoolInstance(){

        Properties propertiesFile = new Properties();
        String path = Thread.currentThread().getContextClassLoader().getResource("application.properties").getPath();
        try {
            propertiesFile.load(new FileInputStream(path));
        } catch (IOException e) {
            e.printStackTrace();
        }

        String redisHost = propertiesFile.getProperty("redis.redisHost");
        int redisPort = Integer.parseInt(propertiesFile.getProperty("redis.redisPort"));
        int redisTimeout = Integer.parseInt(propertiesFile.getProperty("redis.redisTimeout"));
        String password = propertiesFile.getProperty("redis.password");

        if(null == jedisPool){
            synchronized (RedisClientJava.class) {
                if(null == jedisPool){
                    JedisPoolConfig poolConfig = new JedisPoolConfig();
                    jedisPool = new JedisPool(poolConfig , redisHost, redisPort, redisTimeout ,password);
                }
            }
        }
        return jedisPool;
    }


}
