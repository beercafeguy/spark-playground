# spark playground
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
