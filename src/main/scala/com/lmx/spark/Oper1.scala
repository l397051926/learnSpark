package com.lmx.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: lmx
 * @create: 2020/8/10
 **/
object Oper1 {
	/**
	 * map 算子的应用 是在rdd 上面每个分区上运行
	 * 处理是一个分区一个分区的执行
	 * @param args
	 */
	def main(args: Array[String]): Unit = {
		val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
		val sc = new SparkContext(config)
		val listRdd: RDD[Int] = sc.makeRDD(1 to 10)
		val mapRdd: RDD[Int] = listRdd.map(_*2)
		mapRdd.collect().foreach(println)
	}

}
