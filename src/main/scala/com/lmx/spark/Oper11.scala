package com.lmx.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: lmx
 * @create: 2020/8/11
 **/
object Oper11 {

	def main(args: Array[String]): Unit = {
		val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Oper11")
		val sc = new SparkContext(config)
		val listRDD1: RDD[Int] = sc.makeRDD(1 to 5)
		val listRDD2: RDD[Int] = sc.makeRDD(6 to 10)
		val resultRDD: RDD[Int] = listRDD1.union(listRDD2)
		resultRDD.collect().foreach(println)

	}

}
