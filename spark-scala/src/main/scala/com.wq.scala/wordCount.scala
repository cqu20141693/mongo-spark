import org.apache.spark.{SparkConf, SparkContext}

object wordCount{
  def main(args:Array[String]): Unit ={
    println("----------------------------------")
    val conf = new SparkConf().setAppName("wordCount")
    //setMaster("local") 本机的spark就用local，远端的就写ip
    //如果是打成jar包运行则需要去掉 setMaster("local")因为在参数中会指定。
    conf.setMaster("local")
    val sc= new SparkContext(conf)
    val line=sc.textFile("D:\\software\\spark-2.3.1-bin-hadoop2.7\\spark-2.3.1-bin-hadoop2.7\\README.md")
    println(line.count()+"   wqasfdasdfa包含a的个数："+"wqasfdasdfa".count(_=='a'))

    val inputRDD=sc.textFile("D:\\data\\log\\log.txt")
    val errorsRdd=inputRDD.filter(line=>line.contains("INFO"))
    println("Input had "+errorsRdd.count()+" concerning lines")
    println("Here are 10 examples:")
    errorsRdd.take(10).foreach(println)
    println(errorsRdd.first())
    println("----------------------------------")
  }
}