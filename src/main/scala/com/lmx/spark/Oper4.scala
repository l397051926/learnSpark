package com.lmx.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: lmx
 * @create: 2020/8/10
 **/
object Oper4 {

	def main(args: Array[String]): Unit = {
		val config = new SparkConf().setMaster("local[*]").setAppName("oper4")
		val sc = new SparkContext(config)
		val listRDD: RDD[Int] = sc.makeRDD(1 to 10, 4)
		val glomRDD: RDD[Array[Int]] = listRDD.glom()
		glomRDD.collect().foreach( array =>{
			println(array.mkString(","))
		} )
	}

}
