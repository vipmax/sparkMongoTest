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
    System.setProperty("hadoop.home.dir", "C:\\spark\\winutil")

    val conf = new SparkConf().setAppName("LJProcessing").setMaster("local[*]").set("spark.local.dir", "D:\\spark-1.5.1")
    val sc = new SparkContext(conf)

    val dbName = "SNCrawler"
    val endpoint = "192.168.13.133:27017"


    val postsCollection = "livejournal_posts"
    val postsInputConfig = new Configuration()
    postsInputConfig.set("mongo.input.uri", s"mongodb://$endpoint/$dbName.$postsCollection")
    postsInputConfig.set("mongo.input.fields","{'username': 1 , 'eventTimestamp': 1}")

    val posts = sc.newAPIHadoopRDD(
      postsInputConfig, // Configuration
      classOf[MongoInputFormat], // InputFormat
      classOf[Object], // Key type
      classOf[BSONObject] // Value type
    )


    val usersCollection = "livejournal_users"
    val usersInputConfig = new Configuration()
    usersInputConfig.set("mongo.input.uri", s"mongodb://$endpoint/$dbName.$usersCollection")
    usersInputConfig.set("mongo.input.fields","{login: 1 , country: 1}")

    val users = sc.newAPIHadoopRDD(
      usersInputConfig, // Configuration
      classOf[MongoInputFormat], // InputFormat
      classOf[Object], // Key type
      classOf[BSONObject] // Value type
    )
    println("posts:")

    val result =
//      sc.parallelize(
        posts
        .map{case (key,doc) => (doc.get("username"), (doc.get("eventTimestamp")))}
//        .take(10000)
//      )
      .groupByKey()
      .join(
//        sc.parallelize(
          users.map{case (key,doc) => (doc.get("login"), (doc.get("country")))}
//            .take(10000)
//        )
      )
        //       (0010010,(CompactBuffer(1081978740, 1082151360, 1083076680, 1083794520),CX))
      .map((tuple: (AnyRef, (Iterable[AnyRef], AnyRef))) => (tuple._1, new BasicDBObject().append("country",tuple._2._2).append("eventTimestamps", tuple._2._1.toArray)))
//      .take(10)
//      .foreach(println)

//     Create a separate Configuration for saving data to MongoDB.
    var testcollection = "tmp"

    val mongoOutputConfig = new Configuration()
    mongoOutputConfig.set("mongo.output.uri", s"mongodb://$endpoint/$dbName.$testcollection")

//     Save this RDD as a Hadoop "file".
//     The path argument is unused; all documents will go to "mongo.output.uri".
    result.saveAsNewAPIHadoopFile(
      "file:///this-is-completely-unused",
      classOf[Object],
      classOf[BSONObject],
      classOf[MongoOutputFormat[Object, BSONObject]],
      mongoOutputConfig
    )


//
//
//    println("users:")
//    users.take(100).foreach(println(_))

  }
}