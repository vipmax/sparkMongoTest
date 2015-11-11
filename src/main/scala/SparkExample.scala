import com.mongodb.hadoop.{ MongoInputFormat, MongoOutputFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.BSONObject

object SparkExample {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("test").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val dbName = "test"
    val collectionName = "tweets"

    // Set up the configuration for reading from MongoDB.
    val mongoConfig = new Configuration()

    // MongoInputFormat allows us to read from a live MongoDB instance.
    mongoConfig.set("mongo.input.uri", s"mongodb://localhost:27017/$dbName.$collectionName")

    // Create an RDD from MongoDB collection.
    val documents = sc.newAPIHadoopRDD(
      mongoConfig, // Configuration
      classOf[MongoInputFormat], // InputFormat
      classOf[Object], // Key type
      classOf[BSONObject] // Value type
    )

    // Println all doc from collection
    documents.foreach(println)




    val testcollection = "testCollection"

    // Create a separate Configuration for saving data to MongoDB.
    val outputConfig = new Configuration()
    outputConfig.set("mongo.output.uri", s"mongodb://localhost:27017/$dbName.$testcollection")

    // Save this RDD as a Hadoop "file".
    // The path argument is unused; all documents will go to "mongo.output.uri".
    documents.saveAsNewAPIHadoopFile(
      "file:///this-is-completely-unused",
      classOf[Object],
      classOf[BSONObject],
      classOf[MongoOutputFormat[Object, BSONObject]],
      outputConfig
    )

  }
}