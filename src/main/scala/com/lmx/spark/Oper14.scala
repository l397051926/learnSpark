package com.lmx.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author: lmx
 * @create: 2020/8/11
 **/
object Oper14 {

	def main(args: Array[String]): Unit = {
		val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Oper14")
		val sc = new SparkContext(config)
		val arr = Array("one", "one", "two", "two", "two", "three", "three", "three", "three")
		val arrRDD: RDD[String] = sc.makeRDD(arr)
		val groupRDD: RDD[(String, Iterable[Int])] = arrRDD.map((_,1)).groupByKey()
		val countRDD: RDD[(String, Int)] = groupRDD.map(x=> (x._1, x._2.sum))
		countRDD.collect().foreach(println)
	}

}
