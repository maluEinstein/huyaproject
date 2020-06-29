import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{CountVectorizer, HashingTF, IDF, QuantileDiscretizer, StringIndexer, Tokenizer, VectorAssembler}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.language.implicitConversions
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

object MLTest {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)


  def MakeDF(path: String, spark: SparkSession, sc: SparkContext): DataFrame = {
    val data = sc.textFile(path)
    val RDD = data.map(line => line.split("&")).filter(line => line.length == 8)
    val rowRDD = RDD.map(p => Row(p(0), whichDayInWeek(p(0)), isFestival(p(0)), isWeekend(p(0)), p(1).toInt, p(2), p(4), p(5).toInt, p(6), p(7).toInt))
    val schema = StructType(List(StructField("day", StringType, true), StructField("week", IntegerType, true), StructField("Festival", StringType, true)
      , StructField("isWeekend", IntegerType, true), StructField("hour", IntegerType, true), StructField("game_name", StringType, true)
      , StructField("room_name", StringType, true), StructField("room_id", IntegerType, true), StructField("gamer_name", StringType, true)
      , StructField("room_hot", IntegerType, true)))
    val dataDF = spark.createDataFrame(rowRDD, schema)
    val afterDF = dataDF.groupBy("day", "week", "hour", "room_id", "game_name", "room_name", "gamer_name", "Festival", "isWeekend")
      .agg("room_hot" -> "max").withColumnRenamed("max(room_hot)", "room_hot")
    afterDF
  }


  def ReadMysqlData(spark: SparkSession, ipaddr: String, table_name: String): DataFrame = {
    val url: String = "jdbc:mysql://localhost:3306/huya?useUnicode=true&characterEncoding=UTF-8&serverTimezone=GMT"
    val colName: String = "hour"
    val lowerBound = 0
    val upperBound = 23
    val numPartions = 8
    val properties: Properties = new Properties()
    properties.setProperty("user", "hadoop")
    properties.setProperty("password", "Hadoop@123")
    properties.setProperty("driver", "com.mysql.jdbc.Driver")
    val dataDF: DataFrame = spark.read.jdbc(url, table_name, colName, lowerBound, upperBound, numPartions, properties)
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
    var res = 0
    dayOfWeek.setTime(fm) //为什么用val可以？（待解决）
    val week = dayOfWeek.get(Calendar.DAY_OF_WEEK)
    if (week == 6 || week == 1 || week == 7) {
      res = 1
    } else {
      res = 2
    }
    res
  }

  //根据日期判断是什么节日
  def isFestival(day: String): String = {
    val res = day match {
      case "2020-01-01" => "元旦"
      case "2019-12-24" => "平安夜"
      case "2019-12-25" => "圣诞节"
      case "2020-01-24" => "除夕"
      case "2020-02-08" => "元宵"
      case "2020-02-14" => "情人节"
      case "2020-03-12" => "植树节"
      case "2020-04-04" => "清明节"
      case "2020-04-05" => "清明节"
      case "2020-04-06" => "清明节"
      case _ => "无节日"
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
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //从文件中新建DF
    //    val IDArr = Array(660000, 666888, 660001, 521000, 333003, 660002, 688, 52033, 321321, 290987, 10660, 52033, 103444, 523980, 1352977, 11342412,
    //      880205, 660004, 7911, 199300, 9986, 417964, 13579, 5269, 102411, 102491, 520637, 199300, 110120, 122294)
    //   val useDF= MakeDF("E:\\share\\data",spark, sc).select("day", "week", "hour", "room_id", "game_name", "Festival", "isWeekend", "room_hot")
    //      .filter(line => IDArr.contains(line.getInt(3)))

    //从数据库中读取DF
        val useDF = ReadMysqlData(spark, "localhost", "testdata")
        useDF.show()


    //使用训练好的KMeans模型打标签
        val model = PipelineModel.load("e:\\share\\model\\KMeans")
        val AFKMeansDF = model.transform(useDF)

    //利用逻辑回归训练模型
    //MyLogisticRegression(AFKMeansDF,"e:\\share\\model\\LogisticRegressionModel")
    val LRmodel = PipelineModel.load("e:\\share\\model\\LogisticRegressionModel")
     val res = LRmodel.transform(AFKMeansDF)

    //利用随机森林训练模型
    // MyRandomForest(AFKMeansDF, "e:\\share\\model\\RandomForest")
    // val RFmodel = PipelineModel.load("e:\\share\\model\\RandomForest")
    // val res = RFmodel.transform(AFKMeansDF)

    //模型输出展示和写入数据库
        res.show(1000, truncate = false)

    //    res.select("week", "hour", "room_id", "game_name", "Festival", "isWeekend", "label", "prediction").write.jdbc("jdbc:mysql://localhost:3306/huya?useUnicode=true&characterEncoding=UTF-8&serverTimezone=GMT", "testrf", prop)


    //    TF-IDF的DF
    //    val data = sc.textFile("E:\\share\\TFIDFData\\英雄联盟.txt")
    //    val RowRDD = data.map(line => line.split("&")).map(line => Row(line(0), line(1)))
    //    val schema = StructType(List(StructField("game_name", StringType, true), StructField("sentence", StringType, true)))
    //    val DF = spark.createDataFrame(RowRDD, schema)
    //    DF.show()
  }


  //QuantileDiscretizer分类算法
  def MyQuantileDiscretizer(dataDF: DataFrame): DataFrame = {
    val discretizer = new QuantileDiscretizer().setInputCol("room_hot").setOutputCol("label").setNumBuckets(20)
    val model = discretizer.fit(dataDF)
    model.transform(dataDF)
  }

  //K-means聚类算法
  def MyKmeans(df: DataFrame): DataFrame = {
    //做一步特征处理吧热度信息制作成特诊向量输入模型
    val ve = new VectorAssembler().setInputCols(Array("room_hot")).setOutputCol("Feature_room_hot")
    val discretizer = new KMeans().setK(6).setFeaturesCol("Feature_room_hot").setPredictionCol("label").setMaxIter(1000)
    //训练模型
    val pipeline = new Pipeline().setStages(Array(ve, discretizer))
    val model = pipeline.fit(df)
    //模型的保存和加载
    model.write.overwrite().save("e:\\share\\model\\KMeans")
    //val model=KMeansModel.load("e:\\share\\model\\KMeans")
    //打印模型信息
    model.transform(df)
  }

  //根据K-means聚类做最终ML表
  //逻辑回归预测直播热度
  def MyLogisticRegression(df: DataFrame, Path: String): DataFrame = {
    //转换game_name列
    val ALname = new StringIndexer().setInputCol("game_name").setOutputCol("Feature_game_name")
    //转换room_id列
    val ALroom_id = new StringIndexer().setInputCol("room_id").setOutputCol("Feature_room_id")
    //转换hour列
    val ALhour = new StringIndexer().setInputCol("hour").setOutputCol("Feature_hour")
    //转换节日列
    val ALFestival = new StringIndexer().setInputCol("Festival").setOutputCol("Feature_Festival")
    val ve = new VectorAssembler().setInputCols(Array("Feature_game_name", "Feature_room_id", "Feature_hour", "Feature_Festival", "week", "isWeekend")).setOutputCol("features")
    val lr = new LogisticRegression().setMaxIter(200).setRegParam(0.0).setElasticNetParam(0.8).setFeaturesCol("features").setLabelCol("label")
    val pipeline = new Pipeline().setStages(Array(ALname, ALroom_id, ALhour, ALFestival, ve, lr))
    val model = pipeline.fit(df)
    model.save(Path)
    model.transform(df)
  }

  def MyRandomForest(dataDF: DataFrame, Path: String): DataFrame = {
    val ALname = new StringIndexer().setInputCol("game_name").setOutputCol("Feature_game_name")
    //转换room_id列
    val ALroom_id = new StringIndexer().setInputCol("room_id").setOutputCol("Feature_room_id")
    //转换hour列
    val ALhour = new StringIndexer().setInputCol("hour").setOutputCol("Feature_hour")
    //转换节日列
    val ALFestival = new StringIndexer().setInputCol("Festival").setOutputCol("Feature_Festival")
    val ve = new VectorAssembler().setInputCols(Array("Feature_game_name", "Feature_room_id", "Feature_hour", "Feature_Festival", "week", "isWeekend")).setOutputCol("features")
    val rf = new RandomForestClassifier().setFeaturesCol("features").setLabelCol("label").setNumTrees(50).setMaxDepth(30).setMaxBins(50)
    val pipeline = new Pipeline().setStages(Array(ALname, ALroom_id, ALhour, ALFestival, ve, rf))
    val model = pipeline.fit(dataDF)
    //    model.save(Path)
    model.transform(dataDF).select("week", "hour", "room_id", "game_name", "Festival", "isWeekend", "label", "prediction")
  }

  //TF-IDF热词统计
  def test4(df: DataFrame): Unit = {
    val dataDF = df
    //分词器按空格分开句子中词
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(dataDF)

    //将分好的每次词给予一个hash值作为特征向量
    // 使用HashTF作为特征转换工具
    //    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(5000)
    //    val featurizedData = hashingTF.transform(wordsData)

    //视同CountVectorizer作为转换工具
    val cvModel = new CountVectorizer().setInputCol("words").setOutputCol("rawFeatures").setVocabSize(8000).setMinDF(3.0).fit(wordsData)
    val featurizedData = cvModel.transform(wordsData)
    val voc = cvModel.vocabulary
    featurizedData.show()

    //将处理好的特征输入IDF算法得到模型
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rData = idfModel.transform(featurizedData).select("features")
    //    rData.show(100, truncate = false)

  }
}

