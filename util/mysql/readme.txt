#说明
这是一个mysql数据库

#运行mysql容器，本地创建容器映射：数据目录、配置文件目录
docker run --name mysql -v /Users/liufukang/app/mysql5.7/data:/var/lib/mysql -v /Users/liufukang/app/mysql5.7/conf:/etc/mysql/mysql.conf.d -e MYSQL_ROOT_PASSWORD=123456 -d -p 3306:3306 mysql:5.7 

#通过容器连接mysql数据库，也可在本地直接连接
docker run -it --rm mysql:5.7 mysql -hhost.docker.internal -P 3306 -uroot -p