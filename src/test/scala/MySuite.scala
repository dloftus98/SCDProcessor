import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.sql.types.{DataTypes, DecimalType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import java.sql.Timestamp

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, DataFrameSuiteBaseLike, SharedSparkContext}
import org.apache.spark.sql.{DataFrame, Row}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.math.BigDecimal
import scala.io.Source

class MySuite extends FunSuite with DataFrameSuiteBase {

//  @transient var sc: SparkContext = null
//  @transient var hiveContext: HiveContext = null
  var dimDf: DataFrame = null;
  var stageDf: DataFrame = null;

  test("Test table creation and summing of counts") {
    val hiveContext = sqlContext

    val test: DecimalType = DataTypes.createDecimalType(5,2)

    val stageSchema = StructType(Seq(
      StructField("account_num", DataTypes.StringType),
      StructField("first_name", DataTypes.StringType),
      StructField("last_name", DataTypes.StringType),
      StructField("city", DataTypes.StringType),
      StructField("children", DataTypes.IntegerType),
      StructField("salary", DataTypes.createDecimalType(10,2)),
      StructField("double_test", DataTypes.DoubleType),
      StructField("timestamp_test", DataTypes.TimestampType)
    ))

    val dimSchema = StructType(Seq(
      StructField("key", DataTypes.IntegerType),
      StructField("account_num", DataTypes.StringType),
      StructField("first_name", DataTypes.StringType),
      StructField("last_name", DataTypes.StringType),
      StructField("city", DataTypes.StringType),
      StructField("children", DataTypes.IntegerType),
      StructField("salary", DataTypes.createDecimalType(10,2)),
      StructField("double_test", DataTypes.DoubleType),
      StructField("begin_date", DataTypes.TimestampType),
      StructField("end_date", DataTypes.TimestampType),
      StructField("version", DataTypes.IntegerType),
      StructField("most_recent", DataTypes.StringType)
    ))

    val stageRows = Seq(
      Row.fromSeq(List("100", "Dan", "Loftus", "Mount Laurel", 2, BigDecimal.apply(50000.00), 123.23, Timestamp.valueOf("2014-12-26 00:00:00"))),
      Row.fromSeq(List("101", "Rob", "Goodman",  "Wilmington", 2, BigDecimal.apply(50000.00), 123.23, Timestamp.valueOf("2014-12-26 00:00:00"))),
      Row.fromSeq(List("102", "Roman", "Feldblum", "Marlton", 2, BigDecimal.apply(50000.00), 123.23, Timestamp.valueOf("2014-12-26 00:00:00")))
    )

    val dimRows = Seq(
      Row.fromSeq(List(1, "100", "Dan", "Loftus", "Mount Laurel", 2, BigDecimal.apply(50000.00), 123.23, Timestamp.valueOf("2014-11-26 00:00:00"), null, 1, "Y")),
      Row.fromSeq(List(2, "101", "Rob", "Goodman", "Mount Laurel", 2, BigDecimal.apply(50000.00), 123.23, Timestamp.valueOf("2014-11-26 00:00:00"), null, 1, "Y")),
      Row.fromSeq(List(3, "102", "Roman", "Feldblum", "Mount Laurel", 2, BigDecimal.apply(50000.00), 123.23, Timestamp.valueOf("2014-11-26 00:00:00"), null, 1, "Y"))
    )

    val stageRdd = hiveContext.sparkContext.parallelize(stageRows)
    val dimRdd = hiveContext.sparkContext.parallelize(dimRows)

    stageDf = hiveContext.createDataFrame(stageRdd, stageSchema)
    dimDf = hiveContext.createDataFrame(dimRdd, dimSchema)

    //    hiveContext.sql("create database if not exists source_db location 'file:///tmp/source_db'")
    //    hiveContext.sql("create database if not exists target_db location 'file:///tmp/target_db'")
    hiveContext.sql("create database if not exists source_db")
    hiveContext.sql("create database if not exists target_db")

    stageDf.write.mode("overwrite").saveAsTable("source_db.stage_table")

    dimDf.write.mode("overwrite").saveAsTable("target_db.target_table")

    hiveContext.sql("select * from source_db.source_table").collect.foreach(println)
    hiveContext.sql("select * from target_db.target_table").collect.foreach(println)

    val configStr = Source.fromFile("./src/test/resources/tableMetadataTest.json")
      .getLines()
      .mkString

    SCDProcessor.run(hiveContext, configStr, "12/26/2014")

    hiveContext.sql("select count(*) from target_db.target_table").show

    val rows = hiveContext.sql("select count(*) from target_db.target_table").collect()(0).getLong(0)

    assert(rows == 23252L, "The row count should be 44000 but " + rows + " were found.")

    //    hiveContext.sql("drop table source_db.source_table")
    //    hiveContext.sql("drop table target_db.target_table")
    //    hiveContext.sql("drop table target_db.output_table")
    //    hiveContext.sql("drop database source_db")
    //    hiveContext.sql("drop database target_db")

    val f: Future[String] = Future {
      Thread.sleep(2000)
      "future value"
    }

    val f2 = f map { s =>
      println("OK!")
      println("OK!")
    }

    Await.ready(f2, 60 seconds)
    println("exit")

    //sc.stop()
  }
}