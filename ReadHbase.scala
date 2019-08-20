package com.orienst.spark.training
import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor }
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client._
import scala.collection.mutable.ListBuffer
import org.apache.spark._
object ReadHbase {
 case class Person(name: String, l_name: String, date: String, comments: String, id: String)
  
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
      val blacklist: Set[Char] = Set('(')
      val list2 =fruits.map(line => line.filterNot(c => blacklist.contains(c)))
    val rdd = sc.parallelize(list2)
    
      import sqlContext.implicits._
      rdd.collect().foreach(println)
      
      val rdd2 = rdd.map(_.split(",")).map(p =>Person(p(0),p(1),p(2),p(3),p(4)))
     // rdd2.collect().foreach(println)
      val df=  rdd2.toDF()
      df.show()
   //  val rdd = df1.
    //val value=result.value().
    //println(Bytes.toString(value))
     
  }
}