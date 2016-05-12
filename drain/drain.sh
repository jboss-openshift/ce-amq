#!/usr/bin/env bash

echo ${AMQ_HOME}

export AMQ_LIB=${AMQ_HOME}/lib
echo ${AMQ_LIB}

java -cp ${AMQ_LIB}/activemq-client-5.11.0.jar:${AMQ_LIB}/slf4j-api-1.7.10.jar:${AMQ_LIB}/geronimo-jms_1.1_spec-1.1.1.jar:${AMQ_LIB}/hawtbuf-1.11.jar:${AMQ_LIB}/geronimo-j2ee-management_1.1_spec-1.0.1.jar:${AMQ_LIB}/jcl-over-slf4j-1.7.10.jar -jar ${AMQ_LIB}/optional/ce-amq-drain-1.0.0-SNAPSHOT.jar
