import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import java.util.{Calendar, Properties}

object WriteToMysql {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  def RDDdeal(sc: SparkContext): Unit = {
    val sdata = sc.textFile("e:\\share\\data\\2019-12-18.txt")
    //val rddData = sc.textFile(textPath).map(line=>line.split("&")).filter(line=>line.length==8)
    val RDD = sdata.map(line => line.split("&")).filter(line => line.length == 8)
    val rowRDD = RDD.map(p => Row(p(0), p(1).toInt, p(2), p(4), p(5).toInt, p(6), p(7).toInt, p(3).toInt))
    val schema = StructType(List(StructField("day", StringType, true), StructField("hour", IntegerType, true),
      StructField("game_name", StringType, true), StructField("room_name", StringType, true),
      StructField("room_id", IntegerType, true), StructField("gamer_name", StringType, true),
      StructField("room_hot", IntegerType, true), StructField("room_in_page", IntegerType, true)))

  }


  def writeToMysql(dataDF: DataFrame, ipaddr: String, tableName: String): Unit = {
    val prop = new Properties()
    prop.put("user", "hadoop")
    prop.put("password", "Hadoop@123")
    prop.put("driver", "com.mysql.jdbc.Driver")
    dataDF.write.mode("append").jdbc("jdbc:mysql://" + ipaddr + ":3306/huya?useSSL=false&useUnicode=true&characterEncoding=UTF-8", "huya." + tableName, prop)
  }

  def ReadMysqlData(spark: SparkSession, ipaddr: String, table_name: String): DataFrame = {
    val dataDF = spark.read.format("jdbc").
      option("url", "jdbc:mysql://" + ipaddr + ":3306/huya?useUnicode=true&characterEncoding=UTF-8&serverTimezone=GMT").
      option("driver", "com.mysql.jdbc.Driver").
      option("dbtable", table_name).
      option("user", "hadoop").
      option("password", "Hadoop@123").
      load()
    dataDF
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
    writeToMysql(afterDF, "127.0.0.1", "basedata")
    println("写入完成")
    //处理数据根据直播分类计算统计数据并写入数据库
    val analsisDF = afterDF.groupBy("day", "hour", "game_name").agg("room_hot" -> "max", "room_hot" -> "min", "room_hot" -> "avg", "room_hot" -> "sum", "room_hot" -> "count")
    //写入时自动新建数据库
    val prop = new Properties()
    prop.put("user", "hadoop")
    prop.put("password", "Hadoop@123")
    prop.put("driver", "com.mysql.jdbc.Driver")
    analsisDF.write.jdbc("jdbc:mysql://localhost:3306/huya?useSSL=false&useUnicode=true&characterEncoding=UTF-8&serverTimezone=GMT", "room_hot_analsis", prop)
  }

  //从统计好的直播分类中做每天的数据指标做统计
  def dayData(spark:SparkSession):Unit={
    val dataDF=ReadMysqlData(spark,"localhost","basedata")
    dataDF.show(100)
//    val finalDF = dataDF.groupBy("day").agg("room_hot" -> "max", "room_hot" -> "min", "room_hot" -> "avg", "room_hot" -> "sum", "room_hot" -> "count").cache()
//     finalDF.show(100)
    //    val prop = new Properties()
//    prop.put("user", "hadoop")
//    prop.put("password", "Hadoop@123")
//    prop.put("driver", "com.mysql.jdbc.Driver")
//    finalDF.write.jdbc("jdbc:mysql://localhost:3306/huya?useSSL=false&useUnicode=true&characterEncoding=UTF-8&serverTimezone=GMT", "room_hot_analsis_day", prop)

  }

  //输入日期返回周几（1表示星期天）
  def whichDayInWeek(day: String): Int = {
    val fm = new SimpleDateFormat("yyyy-MM-dd").parse(day)
    val dayOfWeek = Calendar.getInstance()
    dayOfWeek.setTime(fm) //为什么用val可以？（待解决）
    dayOfWeek.get(Calendar.DAY_OF_WEEK)
  }

  //判断是否是周末（将周五也认为是周末时段）
  def isWeekend(day: String): Boolean = {
    val fm = new SimpleDateFormat("yyyy-MM-dd").parse(day)
    val dayOfWeek = Calendar.getInstance()
    dayOfWeek.setTime(fm) //为什么用val可以？（待解决）
    val week=dayOfWeek.get(Calendar.DAY_OF_WEEK)
    week == 6 || week == 1 || week == 5
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[6]").setAppName("WriteToSQL")
    val sc=new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val data = sc.textFile("e:\\share\\data")
    val RDD = data.map(line => line.split("&")).filter(line => line.length == 8)
    val rowRDD = RDD.map(p => Row(p(0), p(1).toInt, p(2), p(4), p(5).toInt, p(6), p(7).toInt))
    val schema = StructType(List(StructField("day", StringType, true), StructField("hour", IntegerType, true), StructField("game_name", StringType, true)
      , StructField("room_name", StringType, true), StructField("room_id", IntegerType, true), StructField("gamer_name", StringType, true)
      , StructField("room_hot", IntegerType, true)))
    val dataDF = spark.createDataFrame(rowRDD, schema)
    //清洗数据除去歧义数据选最大值最为标准值
    val afterDF = dataDF.groupBy("day", "hour", "room_id", "game_name", "room_name", "gamer_name")
      .agg("room_hot" -> "max").withColumnRenamed("max(room_hot)", "room_hot")
      .select("room_hot","game_name","day","room_name").filter(line=>line.getInt(0)>=1000000)
      .select("game_name","room_name").distinct()

    val prop = new Properties()
    prop.put("user", "hadoop")
    prop.put("password", "Hadoop@123")
    prop.put("driver", "com.mysql.jdbc.Driver")
    afterDF.write.jdbc("jdbc:mysql://localhost:3306/huya?useSSL=false&useUnicode=true&characterEncoding=UTF-8&serverTimezone=GMT", "TFIDFData", prop)

    //写入已存在的数据库
//    writeToMysql(afterDF, "127.0.0.1", "test")
    //    dealTable(spark,"test")
  }
}
