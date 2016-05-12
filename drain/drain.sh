#!/usr/bin/env bash

echo ${AMQ_HOME}

java -cp ${AMQ_HOME}/lib/activemq-client-5.11.0.jar:${AMQ_HOME}/lib/slf4j-api-1.7.10.jar:${AMQ_HOME}/lib/geronimo-jms_1.1_spec-1.1.1.jar:${AMQ_HOME}/lib/hawtbuf-1.11.jar:${AMQ_HOME}/lib/geronimo-j2ee-management_1.1_spec-1.0.1.jar:${AMQ_HOME}/lib/jcl-over-slf4j-1.7.10.jar -jar ${AMQ_HOME}/lib/optional/ce-amq-drain-1.0.0-SNAPSHOT.jar
