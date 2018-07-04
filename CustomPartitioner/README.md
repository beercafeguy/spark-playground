* The result of partitionBy should always be persisted else it will reshuffle the data every time we call the DAG. <br>

* Hash Partitiner : object(key).hashCode() % num of partitions. <br>

* transformation which has the capability to update the key like map() will not preserve the partitioning.

* Check num of partitions:<br>
`rdd.partitions.size` <br>

* change number of partitions <br>
`repartition and coalesce`

* Use of customer partitioner <br>
`pairRdd.partitionBy(new DomainPartitioner).map(_._1).glom().map(_.toSet.toSeq.length).take(5)`
