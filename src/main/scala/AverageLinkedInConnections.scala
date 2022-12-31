import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
 * //Not implemented since something wrong in data
 *
 * @author Kedar Erande
 */
object AverageLinkedInConnections {

  def parseLine(Line: String) = {

    val res = Line.split(",")
    val age = res(2).toInt;
    val con = res(3).toInt;
    (age, res)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "TotalCustomerExpenditure")

    val linkedInData = sc.textFile("/Users/kedarerande/Downloads/moviedata.data") //todo : change data

    //now we are interested in coln age and connections  either we can write named functions or anonymous functions

    //val mappedRes = linkedInData.map(x => (x.split("::")(2).toInt, x.split("::")(3).toInt))

    val mappedRes = linkedInData.map(parseLine)

    //this will gve the res  (33 , 385) , (33,100) , Now we need to find average of these connections for the age
    //since 33 or age will not present on one machine distributed across machines we need to have group of values
    //coming out from different machines then will average those

    val mappedTransformed = mappedRes.map(x => (x._1, (x._2, 1))) // (33, (100,1) ) , (33,(200,1))

    val result = mappedTransformed.collect()

    result.foreach(println)

    //Now lets groupby using reduceByKey

    // val reducedRes = mappedTransformed.reduceByKey(x=>()) // (100,1) => x._1,x._2
    //(200,1) y._1,y._2
    //== (33, (300,2))

    //Now once we got above lets find the average
    //val averageByAge = reducedRes.map(x => (x._1, x._2._1 / x._2._2))

    //val result = averageByAge.collect()

    //result.foreach(println)


  }

}