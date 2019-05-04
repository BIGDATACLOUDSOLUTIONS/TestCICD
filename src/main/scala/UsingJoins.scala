import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j._


object UsingJoins {

  case class Customer (cust_id: Int, name: String)
  case class Txn (trans_id: Long,cust_id: Int, amount: Float)

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val custs = sc.textFile("C:\\Users\\RAJESH\\Desktop\\custs").map(_.split(","))
    val cust_recs = custs.map( r => (r(0).toInt, Customer(r(0).toInt, r(1))))
    //cust_recs.foreach(println)

    val txns = sc.textFile("C:\\Users\\RAJESH\\Desktop\\custs_txns").map(_.split(","))
    val txns_recs = txns.map( r => (r(1).toInt, Txn(r(0).toLong, r(1).toInt, r(2).toFloat)))
    //txns_recs.foreach(println)
    //println(cust_recs.toDebugString)
    /*
     * The lines below are showing various types of joins
     * which are as easy as using a keyword!
     */
    val joind = cust_recs.join(txns_recs)

    //val leftOuterjoind = cust_recs.leftOuterJoin(txns_recs)
    //val cartesianJoined = cust_recs.cartesian(txns_recs)
    //val cogrpd = cust_recs.cogroup(txns_recs)

    joind.foreach(println)
    //leftOuterjoind.foreach(println)
    //cartesianJoined.foreach(println)
    //cogrpd.foreach(println)
  }
}
