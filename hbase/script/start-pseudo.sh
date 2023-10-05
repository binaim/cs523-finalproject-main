#!/bin/sh
$HBASE_HOME/bin/start-hbase.sh
${HBASE_HOME}/bin/hbase-daemon.sh start thrift
tail -f $HBASE_HOME/logs/*
