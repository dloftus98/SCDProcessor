import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext

import java.sql.Timestamp
import java.text.SimpleDateFormat

import scala.io.Source
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON
import java.math.BigDecimal

import org.apache.log4j.Logger

object SCDProcessor {
  val log = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("SCDProcessor")

    val sc = new SparkContext(conf)

    val sqlContext = new HiveContext(sc)

    val run_date = args(0)

    val configStr = Source.fromFile(args(1))
      .getLines()
      .mkString

    run(sqlContext, configStr, run_date)
  }

  def run(sqlContext: SQLContext, config: String, run_date: String) = {

    val scdMetadata = new ScdMetadata(config)

    val as_of_date = new SimpleDateFormat("MM/dd/yyy").parse(run_date)
    val as_of_date_str = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(as_of_date) //"2014-11-26 00:00:00"

//    val sourceSqlText = "select " + scdMetadata.sourceKeys.mkString(", ") + ", " + scdMetadata.sourceFields.mkString(", ") +
//      " from " + scdMetadata.sourceDatabase + "." + scdMetadata.sourceTable + " where run_date='" + run_date + "'"

    val sourceSqlText = "select " + scdMetadata.sourceKeys.mkString(", ") + ", " + scdMetadata.sourceFields.mkString(", ") +
      " from " + scdMetadata.sourceDatabase + "." + scdMetadata.sourceTable

    log.info(sourceSqlText)

    val sourceDf = sqlContext.sql(sourceSqlText)

    //    sqlContext.sql(sourceSqlText).show(50)

    val targetSqlText = "select key, " + scdMetadata.targetKeys.mkString(", ") + ", " + scdMetadata.targetFields.mkString(", ") +
      ", begin_date, end_date, version, most_recent from " + scdMetadata.targetDatabase + "." + scdMetadata.targetTable

    log.info(targetSqlText)

    val targetDf = sqlContext.sql(targetSqlText)

    //    sqlContext.sql(targetSqlText).show(50)

    var max_key = targetDf.agg(Map("key" -> "max")).collect()(0).getInt(0)
    log.info("max_key = " + max_key)

    val targetDfClosedRecs = targetDf.filter(targetDf("end_date").isNotNull) //closed records

    // we're going to rename the columns for the target dim in order to disambiguate after the join
    // spark didn't seem to offer a good way to disambiguate via table aliases after a join
    val columns = targetDf.columns.map(a => a+"_d")
    var targetDfOpenRecs = targetDf.filter(targetDf("end_date").isNull) //open records
    targetDfOpenRecs = targetDfOpenRecs.toDF(columns :_*)

    // store the schema for later use when converting the output RDD back to a DataFrame
    val dimSchema = targetDf.schema
    log.info(dimSchema)

    // rename the keys to match the renamed columns in the target dim table

    // create a Column type that represents a complex equality test for all the keys
    val joinCondition = scdMetadata.targetKeysRenamed
      .zip(scdMetadata.sourceKeys)
      .map{case (c1, c2) => targetDfOpenRecs(c1) === sourceDf(c2)}
      .reduce(_ && _)

    val joined = targetDfOpenRecs.join(sourceDf, joinCondition, "outer")

    //    joined.show()

    val joined2 = joined.flatMap((r => processScd(r, scdMetadata, as_of_date_str)))


    val new_dim = sqlContext.createDataFrame(joined2, dimSchema)

    //val dim_inserts = new_dim.filter(new_dim("key") === null).repartition(1)
    val dim_inserts = new_dim.filter("key is null").repartition(1)

    //dim_inserts.show(50)

    val dim_non_inserts = new_dim.filter("key is not null")

    //dim_non_inserts.show(50)

    val dim_inserts_new = dim_inserts.mapPartitions(iterator => {
      //val indexed = iterator.zipWithIndex.toList
      iterator.zipWithIndex.map(r =>
        Row.fromSeq(
          r._2 + max_key + 1 ::
            scdMetadata.targetKeys.map(f => r._1.getAs[Any](f)) :::
            scdMetadata.targetFields.map(f => r._1.getAs[Any](f)) :::
            List(r._1.getAs("begin_date").asInstanceOf[Timestamp],
              r._1.getAs("end_date").asInstanceOf[Timestamp],
              r._1.getAs("version").asInstanceOf[Int],
              r._1.getAs("most_recent").asInstanceOf[String])
        )
      )
    })

    val unioned = sqlContext.createDataFrame(dim_inserts_new, dimSchema).unionAll(dim_non_inserts).unionAll(targetDfClosedRecs).repartition(5)

    unioned.write.format("parquet").saveAsTable(scdMetadata.targetDatabase + "." + scdMetadata.outputTable)
  }


  def processScd(joinedRow: Row,
                 scdMetadata: ScdMetadata,
                 as_of_date_str: String) :Array[Row] = {

    // log.info(joinedRow.schema.printTreeString())

    var r = Row.empty
    var new_r = Row.empty
    var rowArray: Array[Row] = null;

    // new record
    // keys don't exist in the dim
    val newRecordCondition = scdMetadata.targetKeysRenamed
      .map(k => Option(joinedRow.getAs(k)).isEmpty)
      .reduce(_ && _)

    // no record
    // keys exist in the dim but not the new/source data
    val noRecordCondition = scdMetadata.sourceKeys
      .map(k => Option(joinedRow.getAs(k)).isEmpty)
      .reduce(_ && _)

    // matching records
    // keys match in both data sets
    val matchingRecordCondition = scdMetadata.sourceKeys
      .zip(scdMetadata.targetKeysRenamed)
      .map{case (k1, k2) => joinedRow.getAs(k1) == joinedRow.getAs(k2)}
      .reduce(_ && _)

//    log.info("examining row")

    if (newRecordCondition) {
//      log.info("newRecordCondition " + joinedRow)

      val keyValues = scdMetadata.sourceKeys.map(k => joinedRow.getAs[Any](k))
      val fieldValues = scdMetadata.sourceFields.map(f => joinedRow.getAs[Any](f))
      val scdValues = List(Timestamp.valueOf(as_of_date_str), null, 1, "Y")

      val values = null :: keyValues ::: fieldValues ::: scdValues

//      log.info(values)

      rowArray = Array(Row.fromSeq(values))

    } else if (noRecordCondition) {
//      log.info("noRecordCondition " + joinedRow)

      val keyValues = scdMetadata.targetKeysRenamed.map(k => joinedRow.getAs[Any](k))
      val fieldValues = scdMetadata.targetFieldsRenamed.map(f => joinedRow.getAs[Any](f))
      val scdValues = List(joinedRow.getAs("begin_date_d").asInstanceOf[Timestamp],
        joinedRow.getAs("end_date_d").asInstanceOf[Timestamp],
        joinedRow.getAs("version_d").asInstanceOf[Int],
        joinedRow.getAs("most_recent_d").asInstanceOf[String])

      val values = joinedRow.getAs("key_d").asInstanceOf[Int] :: keyValues ::: fieldValues ::: scdValues

//      log.info(values)

      rowArray = Array(Row.fromSeq(values))

    } else if (matchingRecordCondition) {
//      log.info("matchingRecordCondition " + joinedRow)

      //cat all type 2 and compare

      val type2TargetFields = scdMetadata.type2TargetFieldsRenamed.map(f => processFieldsWithSchema(f, joinedRow)).mkString
//      log.info("type2TargetFieldsCat = " + type2TargetFields)

      val type2SourceFields = scdMetadata.type2SourceFields.map(f => joinedRow.getAs[Any](f)).mkString

      if (type2TargetFields == type2SourceFields) {
//        log.info("type 2 fields match")

        //only if type 2 fields match to we proceed to checking type 1
//        val type1TargetFields = scdMetadata.type1TargetFieldsRenamed.map(f => joinedRow.getAs(f).asInstanceOf[String]).mkString
        val type1TargetFields = scdMetadata.type1TargetFieldsRenamed.map(f => processFieldsWithSchema(f, joinedRow)).mkString
//        val type1SourceFields = scdMetadata.type1SourceFields.map(f => joinedRow.getAs(f).asInstanceOf[String]).mkString
        val type1SourceFields = scdMetadata.type1SourceFields.map(f => processFieldsWithSchema(f,joinedRow)).mkString

        if (type1TargetFields == type1SourceFields) {
//          log.info("type 1 fields match")

          var keyValues = scdMetadata.targetKeysRenamed.map(k => joinedRow.getAs[Any](k))
          var fieldValues = scdMetadata.targetFieldsRenamed.map(f => joinedRow.getAs[Any](f))
          var scdValues = List(joinedRow.getAs("begin_date_d").asInstanceOf[Timestamp],
            joinedRow.getAs("end_date_d").asInstanceOf[Timestamp],
            joinedRow.getAs("version_d").asInstanceOf[Int],
            joinedRow.getAs("most_recent_d").asInstanceOf[String])

          var values = joinedRow.getAs("key_d").asInstanceOf[Int] :: keyValues ::: fieldValues ::: scdValues

          r = Row.fromSeq(values)

          rowArray = Array(r)

        } else {
//          log.info("type 1 fields don't match")

//          var keyValues = scdMetadata.sourceKeys.map(k => joinedRow.getAs(k).asInstanceOf[String])
          var keyValues = scdMetadata.sourceKeys.map(k => joinedRow.getAs[Any](k))
//          var fieldValues = scdMetadata.sourceFields.map(f => joinedRow.getAs(f).asInstanceOf[String])
          var fieldValues = scdMetadata.sourceFields.map(f => joinedRow.getAs[Any](f))
          var scdValues = List(joinedRow.getAs("begin_date_d").asInstanceOf[Timestamp],
            joinedRow.getAs("end_date_d").asInstanceOf[Timestamp],
            joinedRow.getAs("version_d").asInstanceOf[Int],
            joinedRow.getAs("most_recent_d").asInstanceOf[String])

          var values = joinedRow.getAs("key_d").asInstanceOf[Int] :: keyValues ::: fieldValues ::: scdValues
          //log.info(values)

          new_r = Row.fromSeq(values)

          rowArray = Array(new_r)

        }

      } else {
//        log.info("type 2 fields don't match")
        //we don't bother checking type 1 fields in this case because we already know we will take the source row's data

        //close out the old row
        var keyValues = scdMetadata.targetKeysRenamed.map(k => joinedRow.getAs[Any](k))
        var fieldValues = scdMetadata.targetFieldsRenamed.map(f => joinedRow.getAs[Any](f))
        var scdValues = List(joinedRow.getAs("begin_date_d").asInstanceOf[Timestamp],
          Timestamp.valueOf(as_of_date_str), //filling out the end effective date
          joinedRow.getAs("version_d").asInstanceOf[Int],
          "N") //setting the row as not the most recent

        var values = joinedRow.getAs("key_d").asInstanceOf[Int] :: keyValues ::: fieldValues ::: scdValues

        r = Row.fromSeq(values)

        //construct the new row using the source data
        keyValues = scdMetadata.sourceKeys.map(k => joinedRow.getAs[Any](k))
        fieldValues = scdMetadata.sourceFields.map(f => joinedRow.getAs[Any](f))
        scdValues = List(Timestamp.valueOf(as_of_date_str),
          null,
          joinedRow.getAs("version_d").asInstanceOf[Int] + 1,
          "Y")

        values = null :: keyValues ::: fieldValues ::: scdValues
        //log.info(values)

        new_r = Row.fromSeq(values)

        rowArray = Array(r, new_r)

      }

    }
    return rowArray
  }

  def processFieldsWithSchema(f: String, joinedRow: Row) :String = {

    val fieldType = joinedRow.schema.apply(f).dataType.simpleString
//    log.info("schema map = " + fieldType + ", " + f)

    val retString = checkType(f, fieldType, joinedRow)
    return retString
  }

  def checkType(field: String, fieldType: String, row: Row) :String = {

    var retStr: String = null

    if (fieldType == "string") {
//      log.info("Found StringType")
      retStr = row.getAs[String](field)
    }
    else if (fieldType == "int") {
//      log.info("Found IntegerType")
      retStr = row.getAs[Int](field).toString
    } else if (fieldType == "double") {
//      log.info("Found DoubleType")
      retStr = row.getAs[Double](field).toString
    } else if (fieldType.startsWith("decimal")) {
//      log.info("Found DecimalType")
      retStr = row.getAs[java.math.BigDecimal](field).toString
    }

//    log.info("returning " + retStr)
    return retStr
  }
}

class ScdMetadata(val config: String) extends java.io.Serializable {

  @transient lazy val log = Logger.getLogger(getClass.getName)

  val tableMetadata:Map[String, Any] = JSON.parseFull(config)
    .get
    .asInstanceOf[Map[String, Any]]

  log.info(tableMetadata.mkString)

  val sourceDatabase = tableMetadata.get("sourceDatabase").orNull.asInstanceOf[String]
  val targetDatabase = tableMetadata.get("targetDatabase").orNull.asInstanceOf[String]
  val sourceTable = tableMetadata.get("sourceTable").orNull.asInstanceOf[String]
  val targetTable = tableMetadata.get("targetTable").orNull.asInstanceOf[String]
  val outputTable = tableMetadata.get("outputTable").orNull.asInstanceOf[String]
  val keys = tableMetadata.get("keys").orNull.asInstanceOf[List[Map[String,String]]]
  val fieldMappings = tableMetadata.get("fields").orNull.asInstanceOf[List[Map[String, String]]]

  val sourceKeys = keys.flatMap(m => m.keys)
  val targetKeys = keys.flatMap(m => m.values)

  val sourceFields = fieldMappings.flatMap(m => m.get("sourceField"))
  val targetFields = fieldMappings.flatMap(m => m.get("targetField"))

  val targetKeysRenamed = targetKeys.map(k => k + "_d")
  val targetFieldsRenamed = targetFields.map(f => f + "_d")

  var type1SourceFields = new ListBuffer[String]()
  var type2SourceFields = new ListBuffer[String]()
  var type1TargetFields = new ListBuffer[String]()
  var type2TargetFields = new ListBuffer[String]()
  var type1TargetFieldsRenamed = new ListBuffer[String]()
  var type2TargetFieldsRenamed = new ListBuffer[String]()

  for (fieldMapping <- fieldMappings) {
    if (fieldMapping.get("type").orNull == "1") {
      type1SourceFields += fieldMapping.get("sourceField").orNull
      type1TargetFields += fieldMapping.get("targetField").orNull
      type1TargetFieldsRenamed += (fieldMapping.get("targetField").orNull + "_d")
    }
    else if (fieldMapping.get("type").orNull == "2") {
      type2SourceFields += fieldMapping.get("sourceField").orNull
      type2TargetFields += fieldMapping.get("targetField").orNull
      type2TargetFieldsRenamed += (fieldMapping.get("targetField").orNull + "_d")
    }
  }

  log.info(sourceDatabase)
  log.info(targetDatabase)
  log.info(sourceTable)
  log.info(targetTable)
  log.info(outputTable)
  log.info(keys)
  log.info(fieldMappings)
  log.info(type1SourceFields)
  log.info(type1TargetFields)
  log.info(type1TargetFieldsRenamed)
  log.info(type2SourceFields)
  log.info(type2TargetFields)
  log.info(type2TargetFieldsRenamed)
  log.info("sourceKeys = " + sourceKeys)
  log.info("sourceFields = " + sourceFields)
  log.info("targetKeys = " + targetKeys)
  log.info("targetFields = " + targetFields)
}
