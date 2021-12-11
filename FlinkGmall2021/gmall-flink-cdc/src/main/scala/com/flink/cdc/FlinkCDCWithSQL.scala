package com.flink.cdc

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object FlinkCDCWithSQL {

  def main(args: Array[String]): Unit = {
    //1、获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)

    //2、DDL方式建表
    tableEnv.executeSql(
      s"""
         |CREATE TABLE mysql_binlog(
         |	id STRING NOT NULL,
         |	tm_name STRING,
         |	logo_url STRING
         |) WITH (
         |	'connector' = 'mysql-cdc',
         |	'hostname' = 'localhost',
         |	'port' = '3306',
         |	'username' = 'root',
         |	'password' = '123456',
         |	'database-name' = 'flink',
         |	'table-name' = 'base_trademark'
         |)
         |""".stripMargin)

    //3、查询数据
    val table = tableEnv.sqlQuery("select * from mysql_binlog")

    //4、将动态表转换为流
    val stream:DataStream[(Boolean,Row)] = tableEnv.toRetractStream[Row](table)
    stream.print()

    //5、启动任务
    env.execute("FlinkCDCWithSQL")
  }
}
