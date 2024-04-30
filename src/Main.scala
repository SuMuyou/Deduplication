import org.apache.spark.ml.feature.{HashingTF, MinHashLSH, Tokenizer}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, _}
import java.text.DecimalFormat



object Main {
  val homePath = "/user/wangzhaoyang/"
  val inputPath = homePath + "wet/"
  //val inputPath = homePath + "wet/CC-MAIN-20200702045758-20200702075758-00000.warc_out.txt"
  val files: Seq[String] = Seq(
    inputPath + "CC-MAIN-20200702045758-20200702075758-00000.warc_out.txt",
    inputPath + "CC-MAIN-20200702045758-20200702075758-00001.warc_out.txt",
    inputPath + "CC-MAIN-20200702045758-20200702075758-00002.warc_out.txt",
    inputPath + "CC-MAIN-20200702045758-20200702075758-00003.warc_out.txt",
    inputPath + "CC-MAIN-20200702045758-20200702075758-00004.warc_out.txt",
    //    input_path + "CC-MAIN-20200702045758-20200702075758-00005.warc_out.txt",
    //    input_path + "CC-MAIN-20200702045758-20200702075758-00006.warc_out.txt",
    //    input_path + "CC-MAIN-20200702045758-20200702075758-00007.warc_out.txt",
    //    input_path + "CC-MAIN-20200702045758-20200702075758-00008.warc_out.txt",
    //    input_path + "CC-MAIN-20200702045758-20200702075758-00009.warc_out.txt",
  )
  val outputPath = homePath + "wet_out/"
  val hashNum = 20
  val featureNum = 1000
  val rawSchema = StructType(
    Array(
      StructField("id", StringType, nullable = false),
      StructField("length", IntegerType, nullable = true),
      StructField("start", IntegerType, nullable = true),
      StructField("end", IntegerType, nullable = true),
      StructField("content", StringType, nullable = true),
    )
  )
  val webSchema = StructType(
    Array(
      StructField("webs", StringType, nullable = false)
    )
  )
  """获取dataframe的行数，显示前20行"""
  def getCount(dataFrame: DataFrame
               , numRows: Int = 20
               , showFlag: Boolean = true
               , truncateFlag: Boolean = true
              ): Long = {
    val count = dataFrame.count()
    if (showFlag) {
      dataFrame.show(numRows, truncate = truncateFlag)
    }
    val formatter = new DecimalFormat("#,###")
    val countFormat = formatter.format(count)
    println(s"count: $countFormat")
    count
  }
  def spendTime[R](block: => R, text: String = "运行"): R = {
    val startTime = System.currentTimeMillis()
    val result = block
    val endTime = System.currentTimeMillis()
    println(s"$text 时间: ${(endTime - startTime) / 1000} 秒")
    result
  }

  def readData(spark: SparkSession,textFile: RDD[String]): DataFrame = {
    val keyValueTupleRDD = textFile.map(line => {
      val Array(id, length, start, end, content) = line.split(",", 5)
      (id, length, start, end, content)
    })
    val rawRDD = keyValueTupleRDD.map(tuple => Row(tuple._1, tuple._2.toInt, tuple._3.toInt, tuple._4.toInt, tuple._5))
    val rawData = spark.createDataFrame(rawRDD, rawSchema)
    rawData
  }
  def main(args: Array[String]): Unit = {
    println("start------------------------------------------------------------------------------------------")
    val startTime = System.currentTimeMillis()
    val conf: SparkConf = new SparkConf()
      .setAppName("Deduplication")
      .set("spark.executor.memory", "32g")
    val spark: SparkSession = SparkSession.builder()
      .appName("Wet text deduplication")
      .config(conf)
      .getOrCreate()
    import spark.implicits._

    """获取hdfs input路径下所有文件"""
    //val textFile = spark.sparkContext.textFile(inputPath)
    val textFile = spark.sparkContext.textFile(files.mkString(","))
    val rawData = readData(spark, textFile)
    """预处理原始数据"""
    val contentData = rawData.drop("length", "start", "end")
    //    val contentData = rawData
    //    getCount(contentData, showFlag = true)  // 输出原始数据的content数量

    """保存rawData的content精准去重后的id到文件"""
    val newData = contentData.dropDuplicates("content")
    //    val rawID = contentData.select("id")
    //    val leftID = rawID.except(newData.select("id")).sort("id")
    //    getCount(leftID)
    //    leftID.repartition(1).write.mode("overwrite").text(outputPath + "leftID")

    val wordData = new Tokenizer()
      .setInputCol("content")
      .setOutputCol("token")
      .transform(contentData)
      .drop("content")
    val featuredData = new HashingTF()
      .setInputCol("token")
      .setOutputCol("feature")
      .setNumFeatures(featureNum)
      .transform(wordData)
      .drop("token")
    val minHashLsh = new MinHashLSH()
      .setInputCol("feature")
      .setOutputCol("hashes")
      .setNumHashTables(hashNum)
      .setSeed(123)
    val model = minHashLsh
      .fit(featuredData)
    val transformedData = model
      .transform(featuredData)
      .drop("feature")

    val hashesData = transformedData.withColumn("hashes", col("hashes").cast("string"))
    spendTime(
      hashesData
        //      .repartition(1)
        .write.mode("overwrite")
        .parquet(s"${outputPath}hashes")
      , "hash 数据保存"
    )
    //    hashesData.repartition(1)
    //      .write
    //      .format("csv")
    //      .option("header", "true")
    //      .mode("overwrite")
    //      .save(outputPath + "hashes")


    """读取聚类后的结果"""
    //    val parquetFile = spark.read.parquet(s"${outputPath}cluster_web")
    ////    parquetFile.show(10, truncate = false)
    //    parquetFile.take(10).foreach{
    ////    parquetFile.foreach{
    //      row => {
    //        val webIds = row.getString(0).split("\\|")
    //        println(s"web id array: ${webIds.mkString(", ")}")
    //        val webRowNumArray = webIds.map { webId =>
    //          val parts = webId.split("_")
    //          (parts(0), parts(1).toInt)
    //        }
    //        webRowNumArray.take(3).foreach(println)
    //      }
    //    }


    """自定义去重，只保留字节数最多的"""
    //    val windowSpec = Window.partitionBy("hashes").orderBy($"length".desc)
    //    val rowNumData = transformedData.withColumn("row_num", row_number().over(windowSpec))
    //    getCount(rowNumData, numRows = 50, truncateFlag = false)

    //    val deduplicatedData = rowNumData.filter($"row_num" === 1).drop("row_num")
    //    getCount(deduplicatedData)

    """简易严格去重，对Hash值完全相同的行进行去重，官方自带的去重方法"""
    //    val deduplicatedData = transformedData.dropDuplicates("hashes")

    """hashed列vector转换为string保存到文件中"""
    //    deduplicatedData.printSchema()
    //    deduplicatedData.withColumn("hashes", to_json(col("hashes")))
    //      .repartition(1).write
    //      .format("text")
    //      .option("header", "true")
    //      .mode("overwrite")
    //      .save(output_path + "deduplicatedData")

    //    val newID = deduplicatedData.select("id")
    //    val deduplicatedID = rawID.except(newID).sort("id")
    //    getCount(deduplicatedID)
    //    deduplicatedID.repartition(1).write.mode("overwrite").text(output_path + "deduplicatedID.txt")

    println("finish------------------------------------------------------------------------------------------")
    val endTime = System.currentTimeMillis()
    println(s"计算文本hash 时间：${(endTime - startTime) / 1000} 秒")
    spark.stop()
  }
}