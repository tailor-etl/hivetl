#!/bin/bash
cd `dirname $0`
BIN_DIR=`pwd`
cd ..
DEPLOY_DIR=`pwd`
HADOOP=/opt/hadoop/bin/hadoop
LIB_DIR=$DEPLOY_DIR/lib
LIB_JARS=`ls $LIB_DIR|grep .jar|awk '{print "'$LIB_DIR'/"$0}'|tr "\n" ","`

echo $LIB_DIR
echo 'over........'
#export HADOOP_CLASSPATH=$LiB_JARS
echo $1
$HADOOP jar   $DEPLOY_DIR/lib/tailor-1.0.0-SNAPSHOT-release.jar  com.renren.tailor.exec.ExecDriver $1

