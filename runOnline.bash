#!/bin/bash

rm -rf TFOutput AugTFOutput AAVOutput SimMultOutput SimSumOutput AAVSquareSumRootOutput SimMagMultOutput SimilarityOutput
$HADOOP_HOME/bin/hadoop fs -rm -r TFOutput AugTFOutput AAVOutput SimMultOutput SimSumOutput AAVSquareSumRootOutput SimMagMultOutput SimilarityOutput

$HADOOP_HOME/bin/hadoop jar PA2.jar Main online /dataLoc/xyzIsMarkTwain.txt /dataLoc/Output

#$HADOOP_HOME/bin/hadoop fs -get TFOutput
#$HADOOP_HOME/bin/hadoop fs -get AugTFOutput
$HADOOP_HOME/bin/hadoop fs -get AAVOutput
$HADOOP_HOME/bin/hadoop fs -get SimMultOutput
#$HADOOP_HOME/bin/hadoop fs -get SimSumOutput
#$HADOOP_HOME/bin/hadoop fs -get AAVSquareSumRootOutput
#$HADOOP_HOME/bin/hadoop fs -get SimMagMultOutput
#$HADOOP_HOME/bin/hadoop fs -get SimilarityOutput
