# spark playground (version 1.6.0 with Scala version 2.10.5)
Reference of commands for spark shell

#### Start spark shell right way 
```
spark-shell --master yarn-client \
  --conf spark.ui.port=12930 \
  --num-executors 10 \
  --executor-memory 4G \
  --packages com.databricks:spark-csv_2.10:1.5.0
```

`--packages` option in above command will download the dependency for databricks spark CSV parser and add that in classpath. If internet connect is not available, please use `--jars` instead. For using `--jars <jar files location>`, jar files should be vaibale in local system. <br>

#### Get application ID of spark shell <br>
`sc.applicationId`

#### Convert DF to rdd <br>
`val cartesianDF=cartesianRDD.toDF("number1","number2")` <br>
`val cartesianRDD=cartesianDF.rdd`


#### Print properties on console #####
`spark.sql("SET -v").select("key","value").show(20,false)`

Check if a property is modifiable or not. <br>
` spark.conf.isModifiable("spark.sql.shuffle.partitions")` <br>

#### Note from spark docs

Properties set directly on the SparkConf take highest precedence, then flags passed to spark-submit or spark-shell, then options in the spark-defaults.conf file. A few configuration keys have been renamed since earlier versions of Spark; in such cases, the older key names are still accepted, but take lower precedence than any instance of the newer key.
<br>
https://spark.apache.org/docs/latest/configuration.html#dynamically-loading-spark-properties


