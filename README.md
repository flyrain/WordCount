# A simple Hadoop mapreduce example to count words

Copy the input file to HDFS. No need to copy WordCount-1.0-SNAPSHOT.jar to HDFS.

The command to run:
```
    hadoop jar /tmp/WordCount-1.0-SNAPSHOT.jar com.yufeigu.WordCount /tmp/input.txt  /tmp/output
```    
Check the result:
```
    hadoop fs -cat /tmp/output/part-r-00001
```

You can find the WordCount-1.0-SNAPSHOT.jar under local nodemanager directory, but it is renamed to job.jar instead of original name.
