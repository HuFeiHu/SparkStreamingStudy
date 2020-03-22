package com.c503.hufeihu.utils;

import redis.clients.jedis.Jedis;

public class RedisUtils {

    public static void main(String[] args) {
        Jedis jedis=new Jedis("47.102.127.74",6379);
        jedis.set("username","hufeihu");
        String usename=jedis.get("username");
        System.out.println(usename);

        jedis.close();
    }
}
