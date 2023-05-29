mvn clean package -DskipTests
scp core/target/iginx-core-0.6.0-SNAPSHOT/driver/iotdb12/iotdb12-0.6.0-SNAPSHOT.jar root@11.101.17.24:/root/zlz/iginx-docker/iginx-core-0.6.0-SNAPSHOT/driver/iotdb12 &
scp core/target/iginx-core-0.6.0-SNAPSHOT/driver/influxdb/influxdb-0.6.0-SNAPSHOT.jar root@11.101.17.24:/root/zlz/iginx-docker/iginx-core-0.6.0-SNAPSHOT/driver/influxdb &
scp core/target/iginx-core-0.6.0-SNAPSHOT/lib/iginx* root@11.101.17.24:/root/zlz/iginx-docker/iginx-core-0.6.0-SNAPSHOT/lib &
scp client/target/iginx-client-0.6.0-SNAPSHOT/lib/iginx* root@11.101.17.24:/root/zlz/iginx-docker/iginx-client-0.6.0-SNAPSHOT/lib &
wait