
#!/usr/bin/env bash

: ${SUSPEND:='n'}

set -e

#mvn clean package
export KAFKA_JMX_OPTS="-Xdebug -agentlib:jdwp=transport=dt_socket,server=y,suspend=${SUSPEND},address=5005"

/usr/bin/connect-standalone /iceberg/config/connect-avro-docker.properties /iceberg/config/MySinkConnector.properties
