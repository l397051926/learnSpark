package com.lmx.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: lmx
 * @create: 2020/8/10
 **/
object Oper7 {

	def main(args: Array[String]): Unit = {
		val config = new SparkConf().setMaster("local[*]").setAppName("oper4")
		val sc = new SparkContext(config)
		val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
		val sampleRDD: RDD[Int] = listRDD.sample(false, 0.6 , 1)
		sampleRDD.collect().foreach(println)
	}

}
