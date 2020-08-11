package com.lmx.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author: lmx
 * @create: 2020/8/11
 **/
object Oper13 {

	def main(args: Array[String]): Unit = {
		val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Oper13")
		val sc = new SparkContext(config)
		val listRdd: RDD[(String, Int)] = sc.makeRDD(List( ("a",1), ("b",2), ("c",3) ))
		val partitionBy: RDD[(String, Int)] = listRdd.partitionBy(new MyPartitioner(2))
		partitionBy.saveAsTextFile("out")
	}

}

class MyPartitioner(partitions : scala.Int) extends Partitioner{
	override def numPartitions: Int = partitions

	override def getPartition(key: Any): Int = 1
}
