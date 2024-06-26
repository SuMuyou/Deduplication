import Main.{featureNum, getCount, homePath, inputPath, outputPath, rawSchema, readData, webSchema}
import org.apache.spark.ml.feature.{HashingTF, MinHashLSH, Tokenizer}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.io.PrintWriter
import scala.collection.mutable.ListBuffer
object Cluster_Dedup {
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
  def getFileRDDbyName(rddList: List[(String,RDD[String])], fileName: String): RDD[String] = {
    val matchedRDD = rddList.find { case (filePath, _) => filePath.contains(fileName) }
    matchedRDD match {
      case Some((_,rdd)) => rdd
    }
  }
  //  val parquetPath = homePath + "parquet/"
  val parquetPath = s"${outputPath}cluster_web"
  val outputSchema = StructType(
    Array(
      StructField("id", StringType, nullable = false),
      StructField("start", IntegerType, nullable = true),
      StructField("end", IntegerType, nullable = true),
    )
  )
  val rawLineSchema = StructType(
    Array(
      StructField("id", StringType, nullable = false),
      StructField("line",IntegerType,nullable = false),
      StructField("length", IntegerType, nullable = true),
      StructField("start", IntegerType, nullable = true),
      StructField("end", IntegerType, nullable = true),
      StructField("content", StringType, nullable = true),
    )
  )

  def readLineData(spark: SparkSession, textFile: RDD[String]): RDD[(String, String, String, String, String, String)] = {
    val keyValueTupleRDD = textFile.map(line => {
      val Array(id, length, start, end, content) = line.split(",", 5)
      (id, id.split("_").apply(1), length, start, end, content)
    })
    keyValueTupleRDD
  }
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val conf: SparkConf = new SparkConf()
      .setAppName("Cluster deduplicate")
      .set("spark.executor.memory", "32g")
      .set("spark.executor.extraJavaOptions", "-Xss100M")
    val spark: SparkSession = SparkSession.builder()
      .appName("Wet text deduplication")
      .config(conf)
      .getOrCreate()
    import spark.implicits._
    """读取hashes.parquet文件"""
    val parquetFile = spark.read.parquet(parquetPath)
    val parquetRDD = parquetFile.rdd.map(line => line.getString(0))

    val parquetList = parquetRDD.collect().toList
    var rddList: List[(String, org.apache.spark.rdd.RDD[String])] = List()
    for (filePath <- files){
      val rdd = spark.sparkContext.textFile(filePath)
      rddList = rddList :+ (filePath, rdd)
    }

    //    println(s"parquetRDD.count: $parquetRDD.count")
    var outputData = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], outputSchema)
    parquetList.foreach(line => {
      val fileId = line.split('|').map(str => {
        val part = str.split('_')
        (part(0),part(1).toInt)
      }).unzip
      val file_name = fileId._1
      val rowNum = fileId._2
      val zippedFileId = file_name.zip(rowNum)
      val MapFileId = zippedFileId.groupBy(_._1).view.mapValues(_.map(_._2)).toArray

      val ClusterList = MapFileId.flatMap{case (file,lineIds) =>
        val filename =  file.replace(".wet","_out.txt")
        val fileRDD = getFileRDDbyName(rddList, filename)
        //val fileRDD = spark.sparkContext.textFile(s"${homePath}wet/$filename")
        val fileLineDf = readLineData(spark,fileRDD)
        lineIds.map{lineId =>
          val lId = lineId.toInt
          fileLineDf.filter { case(_, index,_,_,_,_) => index.toInt == lId }
        }
      }
//      val Cluster_RDD = spark.sparkContext.parallelize(MapFileId).collect().flatMap{case (file,lineIds) =>
//        val filename =  file.replace(".wet","_out.txt")
//        val fileRDD = getFileRDDbyName(rddList, filename)
//        //val fileRDD = spark.sparkContext.textFile(s"${homePath}wet/$filename")
//        val fileLineDf = readLineData(spark,fileRDD)
//        lineIds.map{lineId =>
//          val lId = lineId.toInt
//          fileLineDf.filter { case(_, index,_,_,_,_) => index.toInt == lId }
//        }
//      }
      val mergedRDD = ClusterList.reduce(_ union _)
      val rawRDD = mergedRDD.map(row => {Row(row._1,row._3.toInt,row._4.toInt,row._5.toInt,row._6)})
      val rawData = spark.createDataFrame(rawRDD,rawSchema)
      rawData.show(1,true) //必须使用show或其他操作中断spark的惰性执行，否则会产生stackOverflow
      val contentData = rawData.drop("length")
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
      val leftId = featuredData.dropDuplicates("feature").drop("feature")
      //leftId.show(truncate = false)
      val rawId = rawData.drop("length", "content")
      val dedupData = rawId.except(leftId).toDF
      dedupData.show(1,true)
    if (dedupData.count > 0) {
      outputData = outputData.union(dedupData)
    }
    })
    //    outputData.repartition(1)
    //      .write
    //      .format("csv")
    //      .option("header", "true")
    //      .mode("overwrite")
    //      .save(outputPath + "deduplicate")
    val outputFile = "/home/wangzhaoyang/wet_out/dedupliate.txt"
    //println("outputData: ")
    //outputData.show(truncate = false)
    val localRows = outputData.collect()
    val writer = new PrintWriter(outputFile)
    try {
      localRows.foreach(row => writer.println(row.mkString("\n")))
    }
    finally {
      writer.close()
    }
    val endTime = System.currentTimeMillis()
    println(s"聚类去重 时间：${(endTime - startTime) / 1000} 秒")
    spark.stop()
  }
}
