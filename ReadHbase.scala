package com.orienst.spark.training
import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor }
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client._
import scala.collection.mutable.ArrayBuffer

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
    val rdd5 = Seq()
     val stringArr = Array("001","002","007") 
     var newData = new Array[String](3)
for(line<- 0 to stringArr.length-1){
    val g = new Get(Bytes.toBytes(stringArr(line)))
  
  
    val result = hTable.get(g)
    val id = result.getValue(Bytes.toBytes("metadata"),Bytes.toBytes("id"))
    val name = result.getValue(Bytes.toBytes(columnFamily2),Bytes.toBytes("name"))
    val l_name = result.getValue(Bytes.toBytes(columnFamily2),Bytes.toBytes("l_name"))
    val data = result.getValue(Bytes.toBytes(columnFamily2),Bytes.toBytes("data"))
    val comments = result.getValue(Bytes.toBytes(columnFamily2),Bytes.toBytes("comments"))
   // println(result2.toString())
      
    val df =(id,name,l_name,data,comments)
     println("WRRRRRRRRRRRRRRRRRONG ROWKEY")
    val df1 = (Bytes.toString(df._1),Bytes.toString(df._2),Bytes.toString(df._3),Bytes.toString(df._4),Bytes.toString(df._5))
     
     newData(line)+= df1
     println("Array _ " + newData(line))
}
  println(newData.length)
  println(newData(0))
  println("Pidarasdasd")
    val rdd = sc.parallelize(newData)
    
     rdd.collect().foreach(println)
     
     
  
   //  val rdd = df1.
    //val value=result.value().
    //println(Bytes.toString(value))
  }
}