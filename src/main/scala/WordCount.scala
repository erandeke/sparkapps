import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kedar Erande
 */
object WordCount {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[*]", "wordcount")

    val input = sc.textFile("/Users/kedarerande/Downloads/spark-word-count.txt")

    val words = input.flatMap(x => x.split(" "))

    val wordCount = words.map(x => (x, 1))

    val res = wordCount.reduceByKey((x, y) => (x + y))

    res.collect().foreach(println)
  }

}
