package ankitproj1.pkg1
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext

object pair {
  def main(Args:Array[String]){
    
    val conf = new SparkConf()
    .setAppName("sampleprogram")
          .setMaster("local")
    val sc = new SparkContext(conf)
    val filerdd = sc.textFile("C:/Users/ankit/OneDrive/Desktop/shakespeare.txt")
  
    // split by word  
    val splitrdd = filerdd.flatMap( f => f.split(" "))
    val firstcharrdd = splitrdd.map(f => f.charAt(0).toString())
    val pairstorerdd = firstcharrdd.map(f => (f,1))
//    val ki = sc.parallelize(rirstcharrdd)
//    val pair = ki.map(f => (f,1))

  // split by char 
    val split1rdd = filerdd.flatMap( f => f.split(""))
    val tuplrdd = split1rdd.map(f => (f,1))
    val reducerdd = tuplrdd.reduceByKey(_ + _)
    
//    val joinrdd = pairstorerdd.leftInnerJoin(reducerdd)
//    joinrdd.collect().foreach(println)

 //  reducerdd.collect().foreach(println)
}

}