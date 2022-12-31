import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kedar Erande
 */
object TopKElements {

  def main(args: Array[String]) {
    val sc = new SparkContext("local[*]", "top-k-elements")

    val input = sc.textFile("/Users/kedarerande/Downloads/spark-word-count.txt")

    val words = input.flatMap(x => x.split(" "))

    val wordCount = words.map(x => (x, 1))

    val res = wordCount.reduceByKey((x, y) => (x + y))

    //we have sortByKey as a transformation function : so we need here sortByValue (which is not a transformation function)
    //so we will convert tuple ("words" ,1) ==> (1,words) using map

    val reversedTuple = res.map(x => (x._2, x._1))

    val sortedResults = reversedTuple.sortByKey(false).map(x => (x._2, x._1)) // false : for ascending since we want top elements

    sortedResults.collect().foreach(println)
  }


}
