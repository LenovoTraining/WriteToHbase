package com.orienst.spark.training
import org.apache.spark._
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes
object WriteToHbase {
  def parseLine(lines: String) = {
    val field = lines.split("\\|", -1)
    val id = field(0)
    val name = field(1)
    val l_name = field(2)
    val data = field(3)
    val comments = field(4)
    (id, name, l_name, data, comments)

  }
  def main(args: Array[String]) {

    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", "127.0.0.1") 
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    val conf = new SparkConf().setAppName("HbaseRead").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val path = "C:/Users/Lenovo y520/Desktop/data.txt"

    val pathLinux = "file:/home/cloudera/Downloads/data.txt"
    
    val columnFamily1 = "metadata"
    val columnFamily2 = "payload"
    val tableName = "spark_hbase_task"
    val rdd1 = sc.textFile(pathLinux)
    val hTable = new HTable(hconf, tableName)
    val rdd2 = rdd1.map(parseLine)
    rdd2.collect().foreach(println)
    val header2 = rdd2.first
    val row2 = rdd2.filter(l => l != header2)
    row2.collect().foreach(println)
    val row3 = row2.map(lines => lines._1)
    row3.collect().foreach(println)

    val row4 = row2.collect()

    for (line <- row4) {
      val p = new Put(Bytes.toBytes("rowkey " + line._1))
      p.addColumn(Bytes.toBytes(columnFamily1), Bytes.toBytes("id"), Bytes.toBytes(line._1))
      p.addColumn(Bytes.toBytes(columnFamily2), Bytes.toBytes("name"), Bytes.toBytes(line._2))
      p.addColumn(Bytes.toBytes(columnFamily2), Bytes.toBytes("l_name"), Bytes.toBytes(line._3))
      p.addColumn(Bytes.toBytes(columnFamily2), Bytes.toBytes("data"), Bytes.toBytes(line._4))
      p.addColumn(Bytes.toBytes(columnFamily2), Bytes.toBytes("comments"), Bytes.toBytes(line._5))

      hTable.put(p)

    }
  }
}