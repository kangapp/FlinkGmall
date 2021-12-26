package com.flink.util;

import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws Exception {

        //查询phoenix之前先查询redis
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        String dimInfoJsonStr = jedis.get(redisKey);
        if(dimInfoJsonStr != null) {
            jedis.expire(redisKey, 24 * 60 * 60);
            jedis.close();
            return JSONObject.parseObject(dimInfoJsonStr);
        }

        //拼接查询语句
        String querySql = "select * from " + tableName + " where id='" + id + "'";

        //查询phoenix
        List<JSONObject> queryList = JdbcUtil.queryList(connection, querySql, JSONObject.class, true);

        JSONObject dimInfoJson = queryList.get(0);

        jedis.set(redisKey, dimInfoJson.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);

        jedis.close();

        //返回结果
        return dimInfoJson;
    }

    public static void delRedisDimInfo(String tableName, String id) {
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        jedis.del(redisKey);
        jedis.close();
    }
}
