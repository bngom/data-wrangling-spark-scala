import collection.mutable.Stack 
import org.scalatest._
import flatspec._ 
import matchers._
import org.apache.spark.sql.SparkSession
  

class LogsSpec extends AnyFlatSpec with should.Matchers {
  
  val spark = SparkSession.builder().appName("testings").master("local").getOrCreate
        
  import spark.implicits._
  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  
  

  "Logs" should "be parsed" in {
    //"1" should be ("0")
  val logs = spark.read.text("access.log.gz")
  val logAsString = logs.map(_.getString(0))
  val R = """^(?<ip>[0-9.]+) (?<identd>[^ ]) (?<user>[^ ]) \[(?<datetime>[^\]]+)\] \"(?<request>[^\"]*)\" (?<status>[^ ]*) (?<size>[^ ]*) \"(?<referer>[^\"]*)\" \"(?<useragent>[^\"]*)\" \"(?<unk>[^\"]*)\"""".r
    logAsString.flatMap(x => R.unapplySeq(x)).count should be (2655452)
    logAsString.filter(x => R.unapplySeq(x).isEmpty).count should be (4598)
  }

  
}