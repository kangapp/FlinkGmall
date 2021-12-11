package com.util;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class CustomerDeserializer implements DebeziumDeserializationSchema<String> {

    /**
     * 封装的数据格式
     * {
     *     "database":"",
     *     "tableName":"",
     *     "type":"cud",
     *     "before":{},
     *     "after":{},
     * }
     */

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //1、创建JSON对象用于存储最终的数据
        JSONObject result = new JSONObject();

        //2、获取库名和表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tableName = fields[2];

        Struct value = (Struct)sourceRecord.value();
        //3、获取"before"数据
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if(before != null){
            Schema beforeSchema = before.schema();
            List<Field> beforeFields = beforeSchema.fields();
            for(Field item : beforeFields){
                Object beforeValue = before.get(item);
                beforeJson.put(item.name(),beforeValue);
            }
        }
        //4、获取"after"数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if(after != null){
            Schema afterSchema = after.schema();
            List<Field> afterFields = afterSchema.fields();
            for(Field item : afterFields){
                Object afterValue = after.get(item);
                afterJson.put(item.name(),afterValue);
            }
        }
        //5、获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String opName = operation.name();
        //6、将字段写入JSON对象
        result.put("database",database);
        result.put("tableName",tableName);
        result.put("before",beforeJson);
        result.put("after",afterJson);
        result.put("type",operation);

        //7、输出数据
        collector.collect(result.toJSONString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
