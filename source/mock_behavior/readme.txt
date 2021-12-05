#说明
这是一个日志模拟生成器，需要启动Spring-boot程序并监听相应的端口

#更新image
docker build -t gmall_behavior .

启动contailner
docker run -it --rm --name behavior_log_producer gmall_behavior