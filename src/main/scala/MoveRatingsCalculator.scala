import org.apache.spark.SparkContext

/**
 * @author Kedar Erande
 */
object MoveRatingsCalculator {

  def main(args: Array[String]) {
    val sc = new SparkContext("local[*]", "ratings-calculator")

    val input = sc.textFile("/Users/kedarerande/Downloads/moviedata.data")

    //we are interested only in ratings column since we need to find rating -> no of people have given

    val recset = input.flatMap(x => x.split("\t")(2)) //select the ratings col

    //map into tuple

    val tuple = recset.map(x => (x, 1))

    //reduce by key

    val reducedratings = tuple.reduceByKey((x, y) => (x + y))

    //collect results

    val results = reducedratings.collect();

    results.foreach(println)


  }

}