package com.lmx.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: lmx
 * @create: 2020/8/11
 **/
object Oper10 {

	def main(args: Array[String]): Unit = {
		val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Oper9")
		val sc = new SparkContext(config)
		val listRDD: RDD[Int] = sc.makeRDD(1 to 16, 4)
		val value: Int = listRDD.max()
		println(value)
	}

}
