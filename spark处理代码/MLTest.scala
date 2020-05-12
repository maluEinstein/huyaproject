import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{QuantileDiscretizer, StringIndexer, VectorAssembler}
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object MLTest {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  def dealData(sc: SparkContext, spark: SparkSession, path: String): DataFrame = {
    val data = sc.textFile(path)
    val RDD = data.map(line => line.split("&")).filter(line => line.length == 8)
    //    val testRDD=RDD.map(line=>line(0))
    val rowRDD = RDD.map(p => Row(p(0),whichDayInWeek(p(0)),isFestival(p(0)),isWeekend(p(0)), p(1).toInt, p(2), p(4), p(5).toInt, p(6), p(7).toInt))
    val schema = StructType(List(StructField("day",StringType,true),StructField("week", IntegerType, true),StructField("Festival", StringType, true)
      ,StructField("isWeekend",IntegerType,true), StructField("hour", IntegerType, true), StructField("game_name", StringType, true)
      , StructField("room_name", StringType, true), StructField("room_id", IntegerType, true), StructField("gamer_name", StringType, true)
      , StructField("room_hot", IntegerType, true)))
    val dataDF = spark.createDataFrame(rowRDD, schema)
    val afterDF = dataDF.groupBy("day","week", "hour", "room_id", "game_name", "room_name", "gamer_name","Festival","isWeekend")
      .agg("room_hot" -> "max").withColumnRenamed("max(room_hot)", "room_hot")
    afterDF
  }

  def ReadMysqlData(spark: SparkSession, ipaddr: String, table_name: String): DataFrame = {
    //    val dataDF = spark.read.format("jdbc").
    //      option("url", "jdbc:mysql://" + ipaddr + ":3306/huya?useUnicode=true&characterEncoding=UTF-8").
    //      option("driver", "com.mysql.jdbc.Driver").
    //      option("dbtable", table_name).
    //      option("user", "hadoop").
    //      option("password", "Hadoop@123").
    //      option("numPartitions ","10").
    //      option("partitionColumn ","huor").
    //      option("lowerBound ","0").
    //      option("upperBound ","24").
    //      load()
    val url: String = "jdbc:mysql://localhost:3306/huya?useUnicode=true&characterEncoding=UTF-8"
    val table = "MLdata"
    val colName: String = "hour"
    val lowerBound = 0
    val upperBound = 23
    val numPartions = 8
    val properties: Properties = new Properties()
    properties.setProperty("user", "hadoop")
    properties.setProperty("password", "Hadoop@123")
    properties.setProperty("driver", "com.mysql.jdbc.Driver")
    val dataDF: DataFrame = spark.read.jdbc(url, table, colName, lowerBound, upperBound, numPartions, properties)
    dataDF
  }

  //根据日期判断是周几
  def whichDayInWeek(day: String): Int = {
    val fm = new SimpleDateFormat("yyyy-MM-dd").parse(day)
    val dayOfWeek = Calendar.getInstance()
    dayOfWeek.setTime(fm) //为什么用val可以？（待解决）
    dayOfWeek.get(Calendar.DAY_OF_WEEK)
  }

  //判断是否是周末（将周五也认为是周末时段）
  def isWeekend(day: String): Int = {
    val fm = new SimpleDateFormat("yyyy-MM-dd").parse(day)
    val dayOfWeek = Calendar.getInstance()
    var res=0
    dayOfWeek.setTime(fm) //为什么用val可以？（待解决）
    val week=dayOfWeek.get(Calendar.DAY_OF_WEEK)
    if(week == 6 || week == 1 || week == 7) {
      res=1
    }else{
      res=2
    }
   res
  }
  //根据日期判断是什么节日
  def isFestival(day: String):String={
    val res=day match{
      case "2020-01-01"=> "元旦"
      case "2019-12-24"=>"平安夜"
      case "2019-12-25"=>"圣诞节"
      case "2020-01-24"=>"除夕"
      case "2020-02-08"=>"元宵"
      case "2020-02-14"=>"情人节"
      case "2020-03-12"=>"植树节"
      case "2020-04-04"=>"清明节"
      case "2020-04-05"=>"清明节"
      case "2020-04-06"=>"清明节"
      case _=>"无节日"
    }
    res
  }

  def main(args: Array[String]): Unit = {
    val prop = new Properties()
    prop.put("user", "hadoop")
    prop.put("password", "Hadoop@123")
    prop.put("driver", "com.mysql.jdbc.Driver")
    val conf = new SparkConf().setMaster("local[8]").setAppName("appName")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).config("spark.sql.broadcastTimeout", "36000").
      config("spark.debug.maxToStringFields", "100").getOrCreate()
    //    val dataDF = ReadMysqlData(spark, "localhost", "test")
//    val trainDF = dealData(sc, spark, "e:\\share\\data2\\").select("day", "hour", "room_id", "game_name", "room_hot")
    val trainDF = dealData(sc, spark, "e:\\share\\data1\\").select("day", "week","hour", "room_id", "game_name", "room_hot","Festival","isWeekend")
    val ve = new VectorAssembler().setInputCols(Array("room_hot")).setOutputCol("Feature_room_hot")
    val useDF = ve.transform(trainDF)
    val model=KMeansModel.load("e:\\share\\model\\KMeans")
    val AFKMeansDF=model.transform(useDF.filter(line=>line.getInt(3)==660000))
    test3(AFKMeansDF).show(1000,truncate = false)
//    res.show(1000, truncate = false)
  }


  //QuantileDiscretizer分类算法
  def test1(dataDF: DataFrame): DataFrame = {
    val discretizer = new QuantileDiscretizer().setInputCol("room_hot").setOutputCol("label").setNumBuckets(20)
    val model = discretizer.fit(dataDF)
    model.transform(dataDF)
  }

  //K-means聚类算法
  def test2(df: DataFrame): DataFrame = {
    //做一步特征处理吧热度信息制作成特诊向量输入模型
    val ve = new VectorAssembler().setInputCols(Array("room_hot")).setOutputCol("Feature_room_hot")
    val useDF = ve.transform(df)
    val discretizer = new KMeans().setK(6).setFeaturesCol("Feature_room_hot").setPredictionCol("label").setMaxIter(1000)
    //训练模型
    val model = discretizer.fit(useDF)
    //模型的保存和加载
    //model.write.overwrite().save("e:\\share\\model\\KMeans")
    //val model=KMeansModel.load("e:\\share\\model\\KMeans")
    //打印模型信息
    for(i<-model.clusterCenters){
      println("getDistanceMeasure：  "+i)
    }
    model.transform(useDF)
  }
  //根据K-means聚类做最终ML表
  //逻辑回归预测直播热度
  def test3(df: DataFrame): DataFrame = {
    //转换game_name列
    val AFname = new StringIndexer().setInputCol("game_name").setOutputCol("Feature_game_name").fit(df).transform(df)
    //转换day列
    val AFday = new StringIndexer().setInputCol("day").setOutputCol("Feature_day").fit(AFname).transform(AFname)
    //转换room_id列
    val AFroom_id = new StringIndexer().setInputCol("room_id").setOutputCol("Feature_room_id").fit(AFday).transform(AFday)
    //转换hour列
    val AFhour = new StringIndexer().setInputCol("hour").setOutputCol("Feature_hour").fit(AFroom_id).transform(AFroom_id)
    //转换节日列
    val FinalDF= new StringIndexer().setInputCol("Festival").setOutputCol("Feature_Festival").fit(AFhour).transform(AFhour)
    val ve = new VectorAssembler().setInputCols(Array("Feature_game_name", "Feature_day", "Feature_room_id", "Feature_hour","Feature_Festival","week","isWeekend")).setOutputCol("features")
    val useDF = ve.transform(FinalDF)
    val lr = new LogisticRegression().setMaxIter(20).setRegParam(0.0).setElasticNetParam(0.8).setFeaturesCol("features").setLabelCol("label")
    val model = lr.fit(useDF)

    model.transform(useDF)
  }

  //TF-IDF热词统计
  def test4(df: DataFrame): Unit = {




  }
}
