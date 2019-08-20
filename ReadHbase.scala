package com.orienst.spark.training
import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor }
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable._
import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
object ReadHbase {
 case class Person(id: String, name: String, l_name: String, date: String, comments: String)
  
  def main(args: Array[String]) {
   
  val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", "127.0.0.1") 
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    val conf = new SparkConf().setAppName("HbaseRead").setMaster("local[*]")
    val sc = new SparkContext(conf)
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val columnFamily1 = "metadata"
    val columnFamily2 = "payload"
    val tableName = "spark_hbase_task"
    val hTable = new HTable(hconf, tableName)
    val newData = new Array[String](3)
    val fruits = new ListBuffer[String]()
     val stringArray = Array("001","002","007")
     for(line <- 0 to stringArray.length-1){
    val g = new Get(Bytes.toBytes(stringArray(line)))
    val result = hTable.get(g)
    val id = Bytes.toString(result.getValue(Bytes.toBytes("metadata"),Bytes.toBytes("id")))
    val name = Bytes.toString(result.getValue(Bytes.toBytes(columnFamily2),Bytes.toBytes("name")))
    val l_name = Bytes.toString(result.getValue(Bytes.toBytes(columnFamily2),Bytes.toBytes("l_name")))
    val data = Bytes.toString(result.getValue(Bytes.toBytes(columnFamily2),Bytes.toBytes("data")))
    val comments = Bytes.toString(result.getValue(Bytes.toBytes(columnFamily2),Bytes.toBytes("comments")))
   // println(result2.toString())
      
    val df =(id,name,l_name,data,comments)
     println("WRRRRRRRRRRRRRRRRRONG ROWKEY")
     
     
     fruits +=df.toString()
     
    
     newData(line)+=df
     }
  val list = fruits.toList
    val charsToClean: List[String] = List("\"", "(", ")", "'")

    val filtered : List[String] = list.map(line => charsToClean.foldLeft(line)(_.replace(_, " ")))
  
 
    //val l1 = List[String](filtered(0),filtered(1),filtered(2))
   
    
      import sqlContext.implicits._
      
      
      val rdd2 =filtered.map(_.split(",")).map(p =>Person(p(0),p(1),p(2),p(3),p(4)))
     // rdd2.collect().foreach(println)
      val df=  rdd2.toDF()
      println("The First DataFrame")
      df.show()
      println("The Second DataFrame")
    val df1 = df.withColumn("date", when(col("date") === "", "Date – 01-01-1900").otherwise(col("date")));
  
  df1.show()
  val df2 = df1.withColumn("comments",when(col("comments")==="","String: NA").otherwise(col("comments")));
      println("The Third DataFrame")                 
      df2.show()
   //  val rdd = df1.
    //val value=result.value().
    //println(Bytes.toString(value))
     
  }
}