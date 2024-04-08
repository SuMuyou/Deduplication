import Clusterdedup.{outputSchema, readLineData}
import Main.{homePath, spendTime}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

object test_OpenAndFilter {
  def test(arr:String): Unit ={
    val conf: SparkConf = new SparkConf()
      .setAppName("Cluster deduplicate")
      .set("spark.executor.memory", "32g")
      .set("spark.executor.extraJavaOptions", "-Xss40M")
    val spark: SparkSession = SparkSession.builder()
      .appName("Wet text deduplication")
      .config(conf)
      .getOrCreate()
    //spark.sparkContext.setCheckpointDir("/user/wangzhaoyang/checkpoint")
    //    import spark.implicits._
    //    """读取hashes.parquet文件"""
    //    val parquetFile = spark.read.parquet(parquetPath)
    //
    //    val parquetRDD = parquetFile.rdd.map(line => line.getString(0))
    //    val parquetString = parquetRDD.take(40)
    val startTime = System.currentTimeMillis()
    //var outputData = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], outputSchema)

    val fileId = arr.split('|').map(str => {val part = str.split('_')
        (part(0),part(1).toInt)
      })
      val groupedFileId = fileId.groupBy(_._1).view.mapValues(_.map(_._2)).toArray
      val Cluster_RDD = spark.sparkContext.parallelize(groupedFileId).collect().flatMap{case (file,lineIds) => val filename =  file.replace(".wet","_out.txt")
        val fileRDD = spark.sparkContext.textFile(s"${homePath}wet/$filename")
        val fileLineDf = readLineData(spark,fileRDD)
        lineIds.map{lineId =>
          val lId = lineId.toInt
          fileLineDf.filter { case(_, index,_,_,_,_) => index.toInt == lId }
        }
      }
    val newRDD = Cluster_RDD.reduce(_ union _)
    newRDD.foreach(println)
    val time2 = System.currentTimeMillis()
    println(s"Open file and filter : ${(time2-startTime)/1000} 秒")
  }

}
