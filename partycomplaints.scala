
package ankitproj1.pkg1
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object partycomplaints {
  def main(Args:Array[String])
  {
  val conf = new SparkConf()
    .setAppName("sampleprogram")
          .setMaster("local")
    val sc = new SparkContext(conf)
    val filerdd = sc.textFile("C:/Users/ankit/OneDrive/Desktop/PartyComplaints.csv")
    val splitrdd = filerdd.map(f => f.split(","))
    val pairrdd = splitrdd.map(f => (f(0),f(1).toInt))
    val grprdd = pairrdd.groupByKey()
    val maprdd = grprdd.map(f => (f._1,f._2.sum))
    val sortrdd = maprdd.sortBy(f => -f._2)
    val sort1rdd = sortrdd.first()
    println(sort1rdd)

    }
}