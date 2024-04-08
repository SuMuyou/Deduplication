import Main.{featureNum, homePath, outputPath, rawSchema, readData, webSchema}
import org.apache.spark.ml.feature.{HashingTF, MinHashLSH, Tokenizer}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, _}
import java.text.DecimalFormat

object Group_Hash {

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val conf: SparkConf = new SparkConf()
      .setAppName("Group Hash")
      .set("spark.executor.memory", "32g")
    val spark: SparkSession = SparkSession.builder()
      .appName("Wet text deduplication")
      .config(conf)
      .getOrCreate()
    import spark.implicits._

    """根据相同的hash值进行聚类，这里可以重新写一个文件,上面部分需要汇总一些文件后,再进行读入"""
    val hashesData = spark.read.parquet(s"${outputPath}hashes")
    val hashesRDD = hashesData.rdd.map(line => (line.getString(0), line.getString(1)))
    val groupData = hashesRDD.groupBy(_._2)
      .filter(line => line._2.size > 1)
      .map(_._2)
      .map(line => line.map(_._1))
    val aggregatedRDD = groupData.map(line => line.mkString("|"))
    val clusterWeb = spark.createDataFrame(aggregatedRDD.map(row => Row(row)), webSchema)
    clusterWeb.write.mode("overwrite").parquet(s"${outputPath}cluster_web")
    val endTime = System.currentTimeMillis()
    println(s"hash聚类 时间：${(endTime - startTime) / 1000} 秒")
    println(f"聚类数量 ${clusterWeb.count()}")
    spark.stop()
  }
}
