import Clusterdedup.parquetPath
import org.apache.spark.ml.feature.{HashingTF, MinHashLSH, Tokenizer}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
object Controler {

  def main(args: Array[String]): Unit ={
    val conf: SparkConf = new SparkConf()
      .setAppName("Cluster deduplicate")
      .set("spark.executor.memory", "32g")
      .set("spark.executor.extraJavaOptions", "-Xss100M")
      .set("spark.driver.extraJavaOptions", "-Xss100M")
    val spark: SparkSession = SparkSession.builder()
      .appName("Wet text deduplication")
      .config(conf)
      .getOrCreate()
    import spark.implicits._
    """读取hashes.parquet文件"""
    val startTime = System.currentTimeMillis()
    val parquetFile = spark.read.parquet(parquetPath)

    val parquetRDD = parquetFile.rdd.map(line => line.getString(0))
    val allCluster = parquetRDD.collect()
    val arr = allCluster
    spark.stop()
    println("--------start-----------")


    Clusterdedup.dedup(arr)
//    val ClusterNum = 30//allCluster.length
//    var n = 20
//    while( n < ClusterNum){
//
//      var x = n + 20
//      if (n + 20 > ClusterNum){
//        x = ClusterNum
//      }
//      println(f"------Cluster ${n} to ${x}------------")
//      val arr = allCluster.view.slice(n,x).toArray
//
//      Clusterdedup.dedup(arr)
//      n = n + 20
//      println("--------over-end------------")
//    }
    println("--------end-----------")
    val endTime = System.currentTimeMillis()
    println(s"total time: ${(endTime - startTime)/1000} s")
  }
}
