package com.lmx.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: lmx
 * @create: 2020/8/10
 **/
object Oper2 {
	/**
	 * map partition 是以一个分区分区的去执行 和map的区别是 一个计算会占用一个executor 而用分区去执行，则省略了去调用executor 执行
	 * map partition 效率是优于 map的效率
	 * @param args
	 */
	def main(args: Array[String]): Unit = {
		val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
		val sc = new SparkContext(config)
		val listRdd: RDD[Int] = sc.makeRDD(1 to 10)
		val mapRdd: RDD[Int] = listRdd.mapPartitions(_.map(_*2))
		mapRdd.collect().foreach(println)
	}

}
