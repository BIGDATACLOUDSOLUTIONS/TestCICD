import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j._

object WordCount {

  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")//.set("Dhadoop.home.dir","C:\\hadoop-2.8.0\\bin")

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext(conf)

    // Read each line of my book into an RDD
    val input = sc.textFile("C:\\Users\\RAJESH\\Google Drive\\Big Data Training\\Spark\\CORE_SPARK\\WordCount\\book.txt")

    // Split into words separated by a space character
    val words = input.flatMap(x => x.split(" "))


    val wordCounts=words.map( x=> (x,1)).reduceByKey(_+_)

    // Count up the occurrences of each word
    //val wordCounts = words.countByValue()
    // Print the results.
    //wordCounts.foreach(println)
    wordCounts.saveAsTextFile("file:///home/azimukangda5500/dataset/result")
  }
}
