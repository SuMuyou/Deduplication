import org.apache.spark.ml.feature.{HashingTF, MinHashLSH, Tokenizer}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._




object dedup{

  val home_path = "/user/wangzhaoyang/"
  val input_path = home_path + "wet/"
  val output_path = home_path + "wet_out/"
  val file_name = "CC-MAIN-20200702045758-20200702075758-00000.warc_out.txt"
  val src_path = input_path + file_name
  val files: Seq[String] = Seq(
    input_path + "CC-MAIN-20200702045758-20200702075758-00000.warc_out.txt",
//        input_path + "CC-MAIN-20200702045758-20200702075758-00001.warc_out.txt",
//        input_path + "CC-MAIN-20200702045758-20200702075758-00002.warc_out.txt",
//        input_path + "CC-MAIN-20200702045758-20200702075758-00003.warc_out.txt",
//        input_path + "CC-MAIN-20200702045758-20200702075758-00004.warc_out.txt",
//        input_path + "CC-MAIN-20200702045758-20200702075758-00005.warc_out.txt",
//        input_path + "CC-MAIN-20200702045758-20200702075758-00006.warc_out.txt",
//        input_path + "CC-MAIN-20200702045758-20200702075758-00007.warc_out.txt",
//        input_path + "CC-MAIN-20200702045758-20200702075758-00008.warc_out.txt",
//        input_path + "CC-MAIN-20200702045758-20200702075758-00009.warc_out.txt",
//    input_path + "CC-MAIN-20200702045758-20200702075758-00010.warc_out.txt",
//    input_path + "CC-MAIN-20200702045758-20200702075758-00011.warc_out.txt",
//    input_path + "CC-MAIN-20200702045758-20200702075758-00012.warc_out.txt",
//    input_path + "CC-MAIN-20200702045758-20200702075758-00013.warc_out.txt",
//    input_path + "CC-MAIN-20200702045758-20200702075758-00014.warc_out.txt",
//    input_path + "CC-MAIN-20200702045758-20200702075758-00015.warc_out.txt",
//    input_path + "CC-MAIN-20200702045758-20200702075758-00016.warc_out.txt",
//    input_path + "CC-MAIN-20200702045758-20200702075758-00017.warc_out.txt",
//    input_path + "CC-MAIN-20200702045758-20200702075758-00018.warc_out.txt",
//    input_path + "CC-MAIN-20200702045758-20200702075758-00019.warc_out.txt",
  )
  val schema = StructType(Array(
    StructField("id", StringType, nullable = false),
    StructField("length", IntegerType, nullable = true),
    StructField("start", IntegerType, nullable = true),
    StructField("end", IntegerType, nullable = true),
    StructField("content", StringType, nullable = true),
  ))
  def getCount(dataFrame: DataFrame, timeFlag: Boolean = false, showFlag: Boolean = false): Long = {
    val startTime = System.currentTimeMillis()
    val count = dataFrame.count()
    if (showFlag){
      dataFrame.show(20)
    }
    val endTime = System.currentTimeMillis()
    println(s"count: $count")
    if (timeFlag){
      println("spend time: " + (endTime - startTime) / 1000 + "s")
    }
    count
  }
  def getFilenames(filePaths: String): Seq[String] = {
    val files: Seq[String] = Seq()
    files
  }
  def main(args: Array[String]): Unit = {
    println("start------------------------------------------------------------------------------------------")
    val startTime = System.currentTimeMillis()
    val conf: SparkConf = new SparkConf()
      .setAppName("Deduplication configuration")
      .set("spark.executor.memory", "6g")
    val spark: SparkSession = SparkSession.builder()
      .appName("Wet text minHash")
      .config(conf)
      .getOrCreate()
    import spark.implicits._

    val textFile: RDD[String] = spark.sparkContext.textFile(files.mkString(","))
    val keyValueTupleRDD = textFile.map(line => {
      val Array(id, length, start, end, content) = line.split(",", 5)
      (id, length, start, end, content)
    })
    val rawData = spark.createDataFrame(keyValueTupleRDD
      .map(tuple => Row(tuple._1,tuple._2.toInt,tuple._3.toInt,tuple._4.toInt,tuple._5)), schema)
    val infoData = rawData.drop("content")
    //val contentData = rawData.drop("start", "end") //保留start-end
    val contentData = rawData
    //getCount(contentData, showFlag = true)
    //    contentData.show(truncate = true)

    """保存rawData的content精准去重后的id到文件"""
    //    val newData = contentData.dropDuplicates("content")
    //    val rawID = contentData.select("id")
    //    val leftID = rawID.except(newData.select("id")).sort("id")
    //    getCount(leftID)
    //    leftID.repartition(1).write.mode("overwrite").text(output_path + "leftID.txt")

    val wordData = new Tokenizer()
      .setInputCol("content")
      .setOutputCol("word")
      .transform(contentData)
      .drop("content")
    val featuredData = new HashingTF()
      .setInputCol("word")
      .setOutputCol("feature")
      .setNumFeatures(1000)
      .transform(wordData)
      .drop("word")
    //featuredData.show(1, truncate = false)
    val minHashLsh = new MinHashLSH()
      .setInputCol("feature")
      .setOutputCol("hashes")
      .setNumHashTables(20)
      .setSeed(123)
    val model = minHashLsh
      .fit(featuredData)
    val transformedData = model
      .transform(featuredData)
      .drop("feature")
    //transformedData.show(1, truncate = false)
    //getCount(transformedData,showFlag = true)
    transformedData.withColumn("hashes",col("hashes").cast("string")).repartition(1).write
      .format("csv")
      .option("header","true")
      .mode("overwrite")
      .save(output_path+"deduplicationResult")

    //查看自带去重方法
    """自定义去重，只保留字节数最多的"""
    val windowSpec = Window.partitionBy("hashes").orderBy($"length".desc)
    val rowNumData = transformedData.withColumn("row_num", row_number().over(windowSpec))
    val deduplicatedData = rowNumData.filter($"row_num" > 1).drop("row_num").repartition(1)
//    getCount(deduplicatedData, showFlag = true)
    deduplicatedData.drop("length")
    """简易严格去重，对Hash值完全相同的行进行去重，官方自带的去重方法"""
    //    val deduplicatedData = transformedData.dropDuplicates("hashes")

    """hashed列vector转换为string保存到文件中"""
    //    deduplicatedData.printSchema()
    //    deduplicatedData.withColumn("hashes", to_json(col("hashes")))
    //      .repartition(1).write
    //      .format("text")
    //      .option("header", "true")
    //      .mode("overwrite")
    //      .save(output_path + "deduplicatedData.txt")

    val cntData = transformedData.groupBy("hashes").count()
    //getCount(cntData)

    //    val newID = deduplicatedData.select("id")
    //    val deduplicatedID = rawID.except(newID).sort("id")
    //    getCount(deduplicatedID)
    //    deduplicatedID.repartition(1).write.mode("overwrite").text(output_path + "deduplicatedID.txt")

    println("finish------------------------------------------------------------------------------------------")
    val endTime = System.currentTimeMillis()
    println(s"运行总时间：${(endTime - startTime)/1000} 秒")
    spark.stop()
  }
}