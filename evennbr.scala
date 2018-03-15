package ankitproj1.pkg1
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object evennbr {
  
  def main(Args:Array[String])
  {
    val conf = new SparkConf()
    .setAppName("sampleprogram")
          .setMaster("local")
    val sc = new SparkContext(conf)
    val rdd =sc.parallelize(Seq(1,2,3,4,5,6,7,8,9,40)) 
//    val numrdd = rdd.map(f => 
    val evenrdd = rdd.filter(f => f%2 == 0 )
    println(evenrdd.collect().mkString(","))
//      
}
    
}