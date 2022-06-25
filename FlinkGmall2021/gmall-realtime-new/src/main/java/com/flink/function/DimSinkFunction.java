package com.flink.function;

import com.alibaba.fastjson.JSONObject;
import com.flink.utils.PhoenixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        String sinkTable = value.getString("sinkTable");
        value.remove("sinkTable");
        PhoenixUtil.insertValues(sinkTable, value);
    }
}
