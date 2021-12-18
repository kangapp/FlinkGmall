# 说明
这是一个phoenix-hbase数据库

# 构建image
docker build -t gmall_phoenix .

#启动contailner
docker run -d --name phoenix -p 2181:2181 -p 60020:60020 -p 60000:60000 -p 8765:8765 gmall_phoenix

#连接
修改host地址，把当前的容器ip映射到localhost