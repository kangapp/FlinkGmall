package com.flink.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flink.bean.TableProcess;
import com.flink.common.GmallConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Slf4j
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private OutputTag<JSONObject> objectOutputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private Connection connection;

    public TableProcessFunction(OutputTag<JSONObject> objectOutputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.objectOutputTag = objectOutputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {

        //1、获取状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String key = jsonObject.getString("tableName") + "-" + jsonObject.getString("type");
        TableProcess tableProcess = broadcastState.get(key);

        if(tableProcess != null) {
            //2、过滤字段
            JSONObject after = jsonObject.getJSONObject("after");
            filterColumn(after, tableProcess.getSinkColumns());

            //3、分流 --将输出表/主题信息写入after
            after.put("sinkTable", tableProcess.getSinkTable());
            if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                //kafka数据，写入主流   事实表
                collector.collect(after);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                //HBase数据，写入侧数据流   维度表
                readOnlyContext.output(objectOutputTag, after);
            }

        } else {
            log.error(key + "组合不存在");
        }

    }

    /**
     *
     * @param after
     * @param sinkColumns
     */
    private void filterColumn(JSONObject after, String sinkColumns) {
        List<String> columns = Arrays.asList(sinkColumns.split(","));
//        Iterator<Map.Entry<String, Object>> iterator = after.entrySet().iterator();
//        while (iterator.hasNext()) {
//            Map.Entry<String, Object> next = iterator.next();
//            if (!columns.contains(next.getKey())) {
//                iterator.remove();
//            }
//        }

        after.entrySet().removeIf(next -> !columns.contains(next.getKey()));
    }

    @Override
    public void processBroadcastElement(String value, Context context, Collector<JSONObject> collector) throws Exception {
        //1、获取并解析数据
        JSONObject jsonObject = JSON.parseObject(value);
        String data = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(data,TableProcess.class);

        //2、建表
        if(TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }

        //3、写入状态，广播出去
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key,tableProcess);

    }

    //建表语句：create table if not exists db.tn(pk varchar primary key,xxx varchar) extend
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        if(sinkPk == null) {
            sinkPk = "id";
        }
        if(sinkExtend == null) {
            sinkPk = "";
        }
        StringBuffer createTableSQL = new StringBuffer("create table if not exists ")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append("(");

        String[] fields = sinkColumns.split(",");
        for(int i = 0; i < fields.length; i++) {
            String field = fields[i];
            if(sinkPk.equals(field)) {
                createTableSQL.append(field).append(" varchar primary key");
            } else {
                createTableSQL.append(field).append(" varchar");
            }

            //判断是否是最后一个字段，如果不是则添加","
            if(i < fields.length - 1) {
                createTableSQL.append(",");
            }
        }
        createTableSQL.append(")").append(sinkExtend);

        //打印建表语句
        System.out.println(createTableSQL);

        //预编译SQL
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(String.valueOf(createTableSQL));
            //执行
            preparedStatement.execute();
        } catch (SQLException throwables) {
            throw new RuntimeException("建表"+sinkTable+"异常");
        } finally {
            if(preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }
}
