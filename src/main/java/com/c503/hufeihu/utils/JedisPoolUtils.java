package com.c503.hufeihu.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisPoolUtils{

    private static String redisIp = "127.0.0.1";
    private static Integer redisPort = 6379;
    private static JedisPool pool;
    private static Integer maxTotal = 300;
    private static Integer maxIdle = 100;
    private static Integer minIdle = 1;
    private static Boolean testOnBorrow = true;
    private static Boolean testOnReturn = true;


    private static void initPool(){
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(maxTotal);
        config.setMaxIdle(maxIdle);
        config.setMinIdle(minIdle);
        config.setTestOnBorrow(testOnBorrow);
        config.setTestOnReturn(testOnReturn);
        config.setBlockWhenExhausted(true);//连接耗尽的时候，是否阻塞，false会抛出异常，true阻塞直到超时。默认为true。
        pool = new JedisPool(config,redisIp,redisPort,1000*2);
    }

    //静态代码块，初始化Redis池
    static{
        initPool();
    }

    public static Jedis getJedis(){
        return pool.getResource();
    }


    public static void close(Jedis jedis){
        jedis.close();
    }

    public static void main(String[] args) {
        Jedis jedis = getJedis();
        System.out.println(jedis.ping());
    }
}
