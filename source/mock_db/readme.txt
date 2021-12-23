#说明
这是一个数据库数据模拟生成器

#更新image
docker build -t gmall_db .

启动contailner
docker run -it --rm --name db_log_producer gmall_db