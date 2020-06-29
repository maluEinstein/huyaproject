import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import java.util.{Calendar, Properties}

object WriteToMysql {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)


  def writeToMysql(dataDF: DataFrame, ipaddr: String, tableName: String): Unit = {
    val prop = new Properties()
    prop.put("user", "hadoop")
    prop.put("password", "Hadoop@123")
    prop.put("driver", "com.mysql.jdbc.Driver")
    dataDF.write.mode("append").jdbc("jdbc:mysql://" + ipaddr + ":3306/huya?useSSL=false&useUnicode=true&characterEncoding=UTF-8", "huya." + tableName, prop)
  }


  //数据清洗+写入数据库+对直播分类计算统计数据
  def clearData(spark: SparkSession, sc: SparkContext): Unit = {
    val data = sc.textFile("e:\\share\\data\\")
    val RDD = data.map(line => line.split("&")).filter(line => line.length == 8)
    val rowRDD = RDD.map(p => Row(p(0), p(1).toInt, p(2), p(4), p(5).toInt, p(6), p(7).toInt))
    val schema = StructType(List(StructField("day", StringType, true), StructField("hour", IntegerType, true), StructField("game_name", StringType, true)
      , StructField("room_name", StringType, true), StructField("room_id", IntegerType, true), StructField("gamer_name", StringType, true)
      , StructField("room_hot", IntegerType, true)))
    val dataDF = spark.createDataFrame(rowRDD, schema)
    //清洗数据除去歧义数据选最大值最为标准值
    val afterDF = dataDF.groupBy("day", "hour", "room_id", "game_name", "room_name", "gamer_name")
      .agg("room_hot" -> "max").withColumnRenamed("max(room_hot)", "room_hot")
    //写入已存在的数据库
    //    writeToMysql(afterDF, "127.0.0.1", "basedata")
    //    println("写入完成")
    //处理数据根据直播分类计算统计数据并写入数据库
    val analsisDF = afterDF.groupBy("day", "hour", "room_id")
      .agg("room_hot" -> "max", "room_hot" -> "min", "room_hot" -> "avg", "room_hot" -> "sum", "room_hot" -> "count")
    //写入时自动新建数据库
    val prop = new Properties()
    prop.put("user", "hadoop")
    prop.put("password", "Hadoop@123")
    prop.put("driver", "com.mysql.jdbc.Driver")
    analsisDF.write.jdbc("jdbc:mysql://localhost:3306/huya?useSSL=false&useUnicode=true&characterEncoding=UTF-8&serverTimezone=GMT", "room_id_data", prop)
  }


  def main(args: Array[String]): Unit = {
    val prop = new Properties()
    prop.put("user", "hadoop")
    prop.put("password", "Hadoop@123")
    prop.put("driver", "com.mysql.jdbc.Driver")
    val conf = new SparkConf().setMaster("local[6]").setAppName("WriteToSQL")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()


    // 制作DF
    //    val data = sc.textFile("e:\\share\\data")
    //    val RowRDD = data.map(line => line.split("&")).filter(line => line.length == 8).map(p => Row(p(0), p(1).toInt, p(2), p(4), p(5).toInt, p(6), p(7).toInt))
    //    val schema = StructType(List(StructField("day", StringType, true), StructField("hour", IntegerType, true), StructField("game_name", StringType, true)
    //      , StructField("room_name", StringType, true), StructField("room_id", IntegerType, true), StructField("gamer_name", StringType, true)
    //      , StructField("room_hot", IntegerType, true)))
    //    spark.createDataFrame(RowRDD, schema).distinct().write
    //      .jdbc("jdbc:mysql://localhost:3306/huya?useSSL=false&useUnicode=true&characterEncoding=UTF-8&serverTimezone=GMT","test",prop)


    //清洗数据除去歧义数据选最大值最为标准值
    clearData(spark, sc)


  }
}
