/<\/configuration>/i\
<property> \
  <name>hbase.regionserver.wal.codec</name> \
  <value>org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec</value> \
</property> \
<property> \
  <name>hbase.region.server.rpc.scheduler.factory.class</name> \
  <value>org.apache.hadoop.hbase.ipc.PhoenixRpcSchedulerFactory</value> \
  <description>Factory to create the Phoenix RPC Scheduler that uses separate queues for index and metadata updates</description> \
</property> \
<property> \
  <name>hbase.rpc.controllerfactory.class</name> \
  <value>org.apache.hadoop.hbase.ipc.controller.ServerRpcControllerFactory</value> \
  <description>Factory to create the Phoenix RPC Scheduler that uses separate queues for index and metadata updates</description> \
</property> \
