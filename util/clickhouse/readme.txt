# 说明
这是一个clickhouse数据库

# 启动clickhouse server,并映射端口号到本地
docker run -d --name clickhouse-server --ulimit nofile=262144:262144 -p 8123:8123 yandex/clickhouse-server

# 启动clickhouse client 连接服务
docker run -it --rm --link clickhouse-server:clickhouse-server yandex/clickhouse-client --host clickhouse-server