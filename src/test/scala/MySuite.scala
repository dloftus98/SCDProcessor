import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import scala.io.Source

class MySuite extends FunSuite with
  BeforeAndAfterEach with BeforeAndAfterAll{

  @transient var sc: SparkContext = null
  @transient var hiveContext: HiveContext = null

  override def beforeAll(): Unit = {

    val envMap = Map[String,String](("Xmx", "512m"))

    val sparkConfig = new SparkConf()
    sc = new SparkContext("local[2]", "unit test", sparkConfig)
    hiveContext = new HiveContext(sc)
  }

  override def afterAll(): Unit = {
    hiveContext.sql("drop table phila_schools.employee_d_v3")
    hiveContext.sql("drop table phila_schools.employees_filtered_v3")
    hiveContext.sql("drop table phila_schools.employee_d_v3_tmp")
    hiveContext.sql("drop database phila_schools")
    sc.stop()
  }

  test("Test table creation and summing of counts") {
    val configStr = Source.fromFile("./src/test/resources/tableMetadataTest.json")
      .getLines()
      .mkString

    hiveContext.sql("create database if not exists phila_schools location 'file:///tmp/phila_schools'")

    val employee_d_v3_df = hiveContext.read.parquet("file:///Users/dloftus/IdeaProjects/SCDExample/src/main/resources/employee_d_v3.parq")
    employee_d_v3_df.write.mode("overwrite").saveAsTable("phila_schools.employee_d_v3")

    val employees_filtered_v3_df = hiveContext.read.parquet("file:///Users/dloftus/IdeaProjects/SCDExample/src/main/resources/employees_filtered_v3.parq")
    employees_filtered_v3_df.write.mode("overwrite").saveAsTable("phila_schools.employees_filtered_v3")

    SCDProcessor.run(hiveContext, configStr, "11/26/2014")

    val rows = hiveContext.sql("select count(*) from phila_schools.employee_d_v3_tmp").collect()(0).getLong(0)

    assert(rows == 23252L, "The row count should be 44000 but " + rows + " were found.")
  }
}