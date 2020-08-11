package com.lmx.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: lmx
 * @create: 2020/8/11
 **/
object Oper12 {

	def main(args: Array[String]): Unit = {
		val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Oper12")
		val sc = new SparkContext(config)
		val listRDD1: RDD[Int] = sc.makeRDD(1 to 5)
		val listRDD2: RDD[Int] = sc.makeRDD(3 to 8)
		val resultRDD: RDD[Int] = listRDD2.subtract(listRDD1)
		resultRDD.collect().foreach(println)
		val inter: RDD[Int] = listRDD1.intersection(listRDD2)
		inter.collect().foreach(println)
	}

}
