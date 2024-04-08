import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
class ScObject {
   val conf = new SparkConf()
     .setAppName("Spark Object")
     .setMaster("spark://super-SYS-7049GP-TRT:7077")
     .set("spark.executor.memory", "32g")
     .set("spark.executor.allowSparkContext", "true")
  val spark = SparkSession.builder()
    .appName("Wet text deduplication")
    .master("spark://super-SYS-7049GP-TRT:7077")
    .config(conf)
    .getOrCreate()
  val sc = spark.sparkContext
}
object ScObject {
  private val scObject = new ScObject
  def getContext()={
    scObject.sc
  }
  def getSparkSession():ScObject={
    scObject
  }

}
