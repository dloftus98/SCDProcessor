import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import java.text.SimpleDateFormat
import scala.io.Source

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import scala.util.parsing.json.JSON


object SCDType2 {

  def main(args: Array[String]) = {

    // Start the Spark context
    val conf = new SparkConf().setAppName("SCDType2")

    val sc = new SparkContext(conf)

    val sqlContext = new HiveContext(sc)
//    sqlContext.refreshTable("phila_schools.employee_d")

    val run_date = args(0)
    val as_of_date = new SimpleDateFormat("MM/dd/yyy").parse(run_date)
    val as_of_date_str = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(as_of_date) //"2014-11-26 00:00:00"

    println(Source.fromFile("/Users/dloftus/IdeaProjects/SCDExample/src/main/resources/tableMetadata.json").getLines().mkString)
    val tableMetadata:Map[String, Any] = JSON.parseFull(Source.fromFile("/Users/dloftus/IdeaProjects/SCDExample/src/main/resources/tableMetadata.json").getLines().mkString).get.asInstanceOf[Map[String, Any]]

    println(tableMetadata.mkString)

    val database = tableMetadata.getOrElse("database", "notfound").asInstanceOf[String]
    val sourceTable = tableMetadata.getOrElse("sourceTable", "notfound").asInstanceOf[String]
    val targetTable = tableMetadata.getOrElse("targetTable", "notfound").asInstanceOf[String]
    val keys1 = tableMetadata.getOrElse("keys", List("notfound")).asInstanceOf[List[Any]]
    val fieldMappings1 = tableMetadata.getOrElse("fields", List("notfound")).asInstanceOf[List[Any]]

    println(database)
    println(sourceTable)
    println(targetTable)
    println(keys1)
    println(fieldMappings1)

    val keys2 = keys1.asInstanceOf[List[Map[String, String]]]
    val fieldMappings2 = fieldMappings1.asInstanceOf[List[Map[String, String]]]

    val sourceKeys = keys2.flatMap(m => m.keys).mkString(", ")
    val targetKeys = keys2.flatMap(m => m.values).mkString(", ")

    val sourceFields = fieldMappings2.flatMap(m => m.get("sourceField")).mkString(", ")
    val targetFields = fieldMappings2.flatMap(m => m.get("targetField")).mkString(", ")

    println(sourceKeys)
    println(sourceFields)
    println(targetKeys)
    println(targetFields)

    val sourceSqlText = "select " + sourceKeys + ", " + sourceFields + " from " + database + "." + sourceTable
    println(sourceSqlText)

    sqlContext.sql(sourceSqlText).show(50)

    val targetSqlText = "select " + targetKeys + ", " + targetFields + ", begin_date, end_date, version, most_recent from " + database + "." + targetTable
    println(targetSqlText)

    sqlContext.sql(targetSqlText).show(50)
  }
}
