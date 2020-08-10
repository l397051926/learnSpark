package com.lmx.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: lmx
 * @create: 2020/8/6
 **/
object WordCount {

	def main(args: Array[String]): Unit = {
		//创建spark Conf
		val conf = new SparkConf().setMaster("local[*]").setAppName("wordCount")

		val sc = new SparkContext(conf)
		val lins: RDD[String] = sc.textFile("input")
		val words: RDD[String] = lins.flatMap(_.split(" "))
		val wordToSum: RDD[(String, Int)] = words.map((_,1)).reduceByKey(_+_)
		wordToSum.collect().foreach(x => println(x))
//		println(sc)

	}

}
