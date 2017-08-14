#!/bin/bash

rm -rf TrainAuthorCountOutput TrainTFOutput TrainAugTFOutput TrainDTFOutput TrainIDFOutput TrainAAVOutput TrainAuthorIDFOutput
$HADOOP_HOME/bin/hadoop fs -rm -r TrainAuthorCountOutput TrainTFOutput TrainAugTFOutput TrainDTFOutput TrainIDFOutput TrainAAVOutput TrainAuthorIDFOutput

$HADOOP_HOME/bin/hadoop jar PA2.jar Main offline /dataLoc/smallGutenberg.txt /dataLoc/Output
#$HADOOP_HOME/bin/hadoop fs -get TrainAugTFOutput
#$HADOOP_HOME/bin/hadoop fs -get TrainAuthorCountOutput
#$HADOOP_HOME/bin/hadoop fs -get TrainDTFOutput
#$HADOOP_HOME/bin/hadoop fs -get TrainIDFOutput
#$HADOOP_HOME/bin/hadoop fs -get TrainAuthorIDFOutput
#$HADOOP_HOME/bin/hadoop fs -get TrainAAVOutput
