import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataTypes, DecimalType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Row}

import scala.math.BigDecimal
import scala.io.Source

class MySuite extends FunSuite with BeforeAndAfterAll {

  @transient var sc: SparkContext = null
  @transient var hiveContext: HiveContext = null
  var dimDf: DataFrame = null;
  var stageDf: DataFrame = null;
  var configStr: String = null;

  override def beforeAll(): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.eventLog.enabled", "true")

    hiveContext = new HiveContext(new SparkContext("local", "unittest", sparkConf))

    val test: DecimalType = DataTypes.createDecimalType(5,2)

    val stageSchema = StructType(Seq(
      StructField("account_number", DataTypes.StringType),
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
      StructField("account_number", DataTypes.StringType),
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

    hiveContext.sql("create database if not exists source_db location 'file:///tmp/source_db'")
    hiveContext.sql("create database if not exists target_db location 'file:///tmp/target_db'")

    stageDf.write.mode("overwrite").saveAsTable("source_db.stage_table")

    dimDf.write.mode("overwrite").saveAsTable("target_db.target_table")

    configStr = Source.fromFile("./src/test/resources/tableMetadataTest.json")
      .getLines()
      .mkString

  }

  override def afterAll(): Unit = {
    hiveContext.sql("drop table source_db.stage_table")
    hiveContext.sql("drop table target_db.target_table")
    hiveContext.sql("drop table target_db.output_table")
    hiveContext.sql("drop database source_db")
    hiveContext.sql("drop database target_db")

//    sc.stop()
  }

  test("Test table creation and summing of counts") {

    SCDProcessor.run(hiveContext, configStr, "12/26/2014")

    hiveContext.sql("select count(*) from target_db.output_table").show

    val rows = hiveContext.sql("select count(*) from target_db.output_table").collect()(0).getLong(0)

    assert(rows == 6L, "The row count should be 6 but " + rows + " were found.")

  }
}