import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.{BasicDBObject, DBObject, MongoClient}
import com.mongodb.hadoop.{ MongoInputFormat, MongoOutputFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.BSONObject
import scala.collection.JavaConversions

object SparkExample {


  def main(args: Array[String]) {


//    only for windows
    System.setProperty("hadoop.home.dir", "C:\\Users\\djvip\\IdeaProjects\\sparkMongoTest\\hadoop")

    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("C:\\Users\\djvip\\IdeaProjects\\sparkMongoTest\\src\\main\\scala\\test\\SparkExample.scala")
    rdd.foreach(println)

//    val dbName = "SNCrawler"
//    val endpoint = "192.168.13.110:27017"
//
//
//    val postsCollection = "VkFest2016_posts"
//    val postsInputConfig = new Configuration()
//    postsInputConfig.set("mongo.input.uri", s"mongodb://$endpoint/$dbName.$postsCollection")
//    postsInputConfig.set("mongo.input.fields","{'username': 1 , 'eventTimestamp': 1}")
//
//    val posts = sc.newAPIHadoopRDD(
//      postsInputConfig, // Configuration
//      classOf[MongoInputFormat], // InputFormat
//      classOf[Object], // Key type
//      classOf[BSONObject] // Value type
//    )
//
//    posts.foreach(println)
//
//
//    val usersCollection = "livejournal_users"
//    val usersInputConfig = new Configuration()
//    usersInputConfig.set("mongo.input.uri", s"mongodb://$endpoint/$dbName.$usersCollection")
//    usersInputConfig.set("mongo.input.fields","{login: 1 , country: 1}")
//
//    val users = sc.newAPIHadoopRDD(
//      usersInputConfig, // Configuration
//      classOf[MongoInputFormat], // InputFormat
//      classOf[Object], // Key type
//      classOf[BSONObject] // Value type
//    )
//    println("posts:")
//
//
////     Create a separate Configuration for saving data to MongoDB.
//    var testcollection = "tmp"
//
//
//
//    val mongoOutputConfig = new Configuration()
//    mongoOutputConfig.set("mongo.output.uri", s"mongodb://$endpoint/$dbName.$testcollection")
//
//    val output = rdd.map(line => (null, new BasicDBObject("line", line)))
//
//    output.saveAsNewAPIHadoopFile(
//      "file:///this-is-completely-unused",
//      classOf[Object],
//      classOf[BSONObject],
//      classOf[MongoOutputFormat[Object, BSONObject]],
//      mongoOutputConfig
//    )
  }
}