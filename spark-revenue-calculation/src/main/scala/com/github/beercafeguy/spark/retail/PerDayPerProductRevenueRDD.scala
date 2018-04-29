package com.github.beercafeguy.spark.retail

import org.apache.spark.{SparkConf, SparkContext}

object PerDayPerProductRevenueRDD {

  def main(args: Array[String]): Unit = {

    //for windows
    val conf=new SparkConf().setMaster(args(1)).setAppName("PerDayPerProductRevenueRDD")
    val baseDir=args(0)
    //for cluster
    //val baseDir="/user/hchandra/data/retail_db/"
    val ordersDir=baseDir+"orders"
    val productsDir=baseDir+"products"
    val orderItemsDir=baseDir+"order_items"
    //val conf=new SparkConf().setMaster("yarn-client").setAppName("PerDayPerProductRevenueRDD")
    val sparkContext=new SparkContext(conf)
    val orders=sparkContext.textFile(ordersDir)
    val orderItems=sparkContext.textFile(orderItemsDir)
    val products=sparkContext.textFile(productsDir)
    val paidOrders=orders.filter( order => order.split(",")(3)=="CLOSED" || order.split(",")(3)=="COMPLETE")
    val paidOrdersMap=paidOrders.map(order => {
      val o=order.split(",")
      (o(0).toInt,order)
    })

    val orderItemsMap=orderItems.map(orderItem => {
      val oi=orderItem.split(",")
      (oi(1).toInt,orderItem)
    })

    val paidOrdersWithProduct=paidOrdersMap.join(orderItemsMap)

    val orderWithProductSubTotal=paidOrdersWithProduct.map(order => {
      val o = order._2._1.split(",")
      val oi=order._2._2.split(",")
      val oDate=o(1).substring(0,10)
      ((oDate,oi(2).toInt),oi(4).toFloat)
    })


    val perProductPerDayRevenue=orderWithProductSubTotal.reduceByKey((total,subtotal)=>total+subtotal).
      map( order => (order._1._1,order._1._2,order._2))

    val perProductPerDayRevenueMap=perProductPerDayRevenue.map(order => (order._2,(order._1,order._3)))

    val productsMap=products.filter(product => product.split(",")(4)!="").map(product => {
      val p = product.split(",")
      (p(0).toInt,p(2))
    })

    val productNameMappedRevenue=perProductPerDayRevenueMap.join(productsMap)
    val revenueProdNameDate=productNameMappedRevenue.map(order => (order._2._1._1,order._2._2,order._2._1._2))

    val sortedByDateAndRevenue=revenueProdNameDate.map(order => {
      (order._3,(order._1,order._2))
    }).sortByKey(false).
      map(order => (order._2._1,(order._2._2,order._1))).
      sortByKey(true).map(order => (order._1,order._2._2,order._2._1))
    val finalPerProductPerDayRevenue=sortedByDateAndRevenue.map(order => order._1+","+order._2+","+order._3)
    finalPerProductPerDayRevenue.saveAsTextFile(baseDir+"product_per_day_revenue")

  }
}