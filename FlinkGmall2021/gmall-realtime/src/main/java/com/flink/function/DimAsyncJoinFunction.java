package com.flink.function;

import com.alibaba.fastjson.JSONObject;

public interface DimAsyncJoinFunction<T> {

    public String getKey(T input);
    public void join(T input, JSONObject dimInfo);
}
