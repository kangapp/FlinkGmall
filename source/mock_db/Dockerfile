FROM openjdk:8
RUN mkdir -p /opt/module/gmall-flink/rt_db/
COPY . /opt/module/gmall-flink/rt_db
WORKDIR /opt/module/gmall-flink/rt_db
CMD ["java", "-jar", "gmall2020-mock-db-2020-11-27.jar"]
