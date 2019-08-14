package com.orienst.spark.training
import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor }
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client._

import org.apache.spark._
object ReadHbase {
  def main(args: Array[String]) {
  val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", "127.0.0.1") 
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    val conf = new SparkConf().setAppName("HbaseRead").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val columnFamily1 = "metadata"
    val columnFamily2 = "payload"
    val tableName = "spark_hbase_task"
    val hTable = new HTable(hconf, tableName)
     
     for(lines <- 0 to 2){
    val g = new Get(Bytes.toBytes("rowkey" + lines))
    val result = hTable.get(g)
    val result2 = result.getValue(Bytes.toBytes("metadata"),Bytes.toBytes("id"))
   // println(result2.toString())
     
    println("NEWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWW")
    val value=result.value()
    println(Bytes.toString(value))}
  }
}