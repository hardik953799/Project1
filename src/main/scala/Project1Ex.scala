import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import java.sql.DriverManager
import java.sql.Connection
import java.util.Scanner
import org.apache.spark.sql.SparkSession
object Project1Ex extends App {
   //import org.apache.spark.sql.SparkSession



  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .getOrCreate()

  val lines = spark.sparkContext.parallelize(
    Seq("Spark Intellij Idea Scala test one",
      "Spark Intellij Idea Scala test two",
      "Spark Intellij Idea Scala test three"))

  val counts = lines
    .flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)

  counts.foreach(println)

}