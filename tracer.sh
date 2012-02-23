#!/bin/bash

if [ "${1}" == "-h" ]; then
    echo 'Usage: tracer.sh [<listenport> [<connecthost> [<connectport>]]]'
    echo '   Serially traces connections on the <listenport>, logging'
    echo '   frames received and passing them to the connect host and port.'
    exit
fi

M2_REPO=~/.m2/repository
AMQP_JAR=$M2_REPO/com/rabbitmq/amqp-client/2.7.1/amqp-client-2.7.1.jar
IO_JAR=$M2_REPO/commons-io/commons-io/1.4/commons-io-1.4.jar

java -cp $AMQP_JAR:$IO_JAR com.rabbitmq.tools.Tracer $*

