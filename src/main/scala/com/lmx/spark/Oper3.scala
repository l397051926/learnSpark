package com.lmx.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: lmx
 * @create: 2020/8/10
 **/
object Oper3 {

	def main(args: Array[String]): Unit = {
		val config = new SparkConf().setMaster("local[*]").setAppName("oper3")
		val sc = new SparkContext(config)
		val listRdd: RDD[List[Int]] = sc.makeRDD(Array(List(1,2), List(3,4)))
		val flatMapRDD: RDD[Int] = listRdd.flatMap(datas => datas)
		flatMapRDD.collect().foreach(println)
	}

}
