import dedup.{files, getCount, output_path}
import org.apache.spark.ml.feature.{HashingTF, MinHashLSH, Tokenizer}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object dedup2 {
  val home_path = "/user/wangzhaoyang/"
  val input_path = home_path + "wet_out/deduplicationResult/"
  val output_path = home_path + "wet_dedup_out/"
  //val file_name = "CC-MAIN-20200702045758-20200702075758-00000.warc_out.txt"
  //val src_path = input_path + file_name
  val files: Seq[String] = Seq(
    input_path + "part-000000.csv",
    input_path + "part-000001.csv",
    //    input_path + "CC-MAIN-20200702045758-20200702075758-00001.warc_out.txt",
    //    input_path + "CC-MAIN-20200702045758-20200702075758-00002.warc_out.txt",
    //    input_path + "CC-MAIN-20200702045758-20200702075758-00003.warc_out.txt",
    //    input_path + "CC-MAIN-20200702045758-20200702075758-00004.warc_out.txt",
    //    input_path + "CC-MAIN-20200702045758-20200702075758-00005.warc_out.txt",
    //    input_path + "CC-MAIN-20200702045758-20200702075758-00006.warc_out.txt",
    //    input_path + "CC-MAIN-20200702045758-20200702075758-00007.warc_out.txt",
    //    input_path + "CC-MAIN-20200702045758-20200702075758-00008.warc_out.txt",
    //    input_path + "CC-MAIN-20200702045758-20200702075758-00009.warc_out.txt",
  )
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
  val schema = StructType(Array(
    StructField("id", StringType, nullable = false),
    StructField("length", IntegerType, nullable = true),
    StructField("start", IntegerType, nullable = true),
    StructField("end", IntegerType, nullable = true),
    StructField("hashes", StringType, nullable = true),
  ))
  def main(args: Array[String]): Unit = {
    println("start------------------------------------------------------------------------------------------")
    val startTime = System.currentTimeMillis()
    val conf: SparkConf = new SparkConf()
      .setAppName("Deduplication configuration2")
      .set("spark.executor.memory", "6g")
    val spark: SparkSession = SparkSession.builder()
      .appName("Wet text deduplication")
      .config(conf)
      .getOrCreate()
    import spark.implicits._

    val dataWithHashes = spark.read
      .option("header","true")
      .schema(schema)
      .format("csv")
      .csv(input_path+"*.csv")
    dataWithHashes.show()
    println(dataWithHashes.count())
    val windowSpec = Window.partitionBy("hashes").orderBy($"length".desc)
    val rowNumData = dataWithHashes.withColumn("row_num", row_number().over(windowSpec))
    val deduplicatedData = rowNumData.filter($"row_num" > 1).drop("row_num").drop("hashes").repartition(1)
    getCount(deduplicatedData, showFlag = true)

    deduplicatedData.write
      .format("csv")
      .option("header","true")
      .mode("overwrite")
      .save(output_path+"deduplicationResult")
  }
}