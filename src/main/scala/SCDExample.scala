import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import java.text.SimpleDateFormat
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}


object SCDExample {

  def main(args: Array[String]) = {

    // Start the Spark context
    val conf = new SparkConf().setAppName("SCDExample")

    val sc = new SparkContext(conf)

    val sqlContext = new HiveContext(sc)
    sqlContext.refreshTable("phila_schools.employee_d")

    val run_date = args(0)
    val as_of_date = new SimpleDateFormat("MM/dd/yyy").parse(run_date)
    val as_of_date_str = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(as_of_date) //"2014-11-26 00:00:00"

    // Read some example file to a test RDD
    // val df = sqlContext.sql("select run_date, count(*) as cnt from phila_schools.school_employees group by run_date order by cnt desc")

    val employee_stg = sqlContext.sql("select last_name, first_name, pay_rate_type, pay_rate, title_description," +
                                      " home_organization, home_organization_description, organization_level," +
                                      " type_of_representation, gender," +
                                      " from_unixtime(unix_timestamp(run_date, 'MM/dd/yyyy')) as as_of_date" +
                                      " from phila_schools.employees where run_date='" + run_date + "'")

    val employee_d_tmp = sqlContext.sql("select * from phila_schools.employee_d")

    var max_key = employee_d_tmp.agg(Map("key" -> "max")).collect()(0).getInt(0)
    println("max_key = " + max_key)

    val employee_d_most_recent = employee_d_tmp.filter(employee_d_tmp("most_recent") === "Y")
    val employee_d_old_recs = employee_d_tmp.filter(employee_d_tmp("most_recent") === "N")

    // process most recent dim records and new incoming records
    val columns = employee_d_most_recent.columns.map(a => a+"_d")


    val renamed_employee_d = employee_d_most_recent.toDF(columns :_*)

    val dimSchema = employee_d_most_recent.schema

//    val joined = renamed_employee_d.join(employee_stg,
//                                    renamed_employee_d("last_name_d")===employee_stg("last_name") &&
//                                    renamed_employee_d("first_name_d")===employee_stg("first_name") &&
//                                    renamed_employee_d("home_organization_d")===employee_stg("home_organization"),
//                                 "outer")

    val df1KeyArray: Array[String] = Array("last_name_d", "first_name_d", "home_organization_d")
    val df2KeyArray: Array[String] = Array("last_name", "first_name", "home_organization")

    val joinExprs = df1KeyArray
      .zip(df2KeyArray)
      .map{case (c1, c2) => renamed_employee_d(c1) === employee_stg(c2)}
      .reduce(_ && _)

    val joined = renamed_employee_d.join(employee_stg, joinExprs, "outer").repartition(5)

    val joined2 = joined.flatMap((r => doStuff(r, as_of_date_str)))

    val new_dim = sqlContext.createDataFrame(joined2, dimSchema)

    //val dim_inserts = new_dim.filter(new_dim("key") === null).repartition(1)
    val dim_inserts = new_dim.filter("key is null").repartition(1)

    //dim_inserts.show(50)

    val dim_non_inserts = new_dim.filter("key is not null")

    //dim_non_inserts.show(50)

    val dim_inserts_new = dim_inserts.mapPartitions(iterator => {
      //val indexed = iterator.zipWithIndex.toList
      iterator.zipWithIndex.map(r =>
        Row(
          r._2 + max_key + 1,
          r._1.getAs("last_name"),
          r._1.getAs("first_name"),
          r._1.getAs("pay_rate_type"),
          r._1.getAs("pay_rate"),
          r._1.getAs("title_description"),
          r._1.getAs("home_organization"),
          r._1.getAs("home_organization_description"),
          r._1.getAs("organization_level"),
          r._1.getAs("type_of_representation"),
          r._1.getAs("gender"),
          r._1.getAs("version"),
          r._1.getAs("begin_date"),
          r._1.getAs("end_date"),
          r._1.getAs("most_recent")
        )
      )
    })

    val unioned = sqlContext.createDataFrame(dim_inserts_new, dimSchema).unionAll(dim_non_inserts).unionAll(employee_d_old_recs)

    //dim_inserts_new_df.show(50)

    //dim_inserts_new_df.write.mode("overwrite").saveAsTable("phila_schools.temp_table_inserts")
    //new_dim.write.mode("overwrite").saveAsTable("phila_schools.temp_table")
    unioned.write.mode("overwrite").saveAsTable("phila_schools.temp_table_union")
    sqlContext.sql("ALTER TABLE phila_schools.employee_d RENAME TO phila_schools.employee_d_pre_" + run_date.replaceAll("/", "_"))
    sqlContext.sql("ALTER TABLE phila_schools.temp_table_union RENAME TO phila_schools.employee_d")
    sqlContext.sql("ALTER TABLE phila_schools.employee_d SET SERDEPROPERTIES ('path' = 'hdfs://ip-10-0-0-172.ec2.internal:8020/user/hive/warehouse/phila_schools.db/employee_d')")

    // val df2 = df.filter(!df("last_name").contains("LAST_NAME")).groupBy("run_date").count()
    // df2.orderBy(desc("count")).show()
  }

  def doStuff(joinedRow: Row, as_of_date_str: String) :Array[Row] = {

    // println(joinedRow.schema.printTreeString())

    var r = Row.empty
    var new_r = Row.empty

    if (joinedRow.getAs("last_name_d") == null &&
        joinedRow.getAs("first_name_d") == null &&
        joinedRow.getAs("home_organization_d") == null) {
      // new employee that didn't exist in the dim
      r = Row(null,
        joinedRow.getAs("last_name"),
        joinedRow.getAs("first_name"),
        joinedRow.getAs("pay_rate_type"),
        joinedRow.getAs("pay_rate"),
        joinedRow.getAs("title_description"),
        joinedRow.getAs("home_organization"),
        joinedRow.getAs("home_organization_description"),
        joinedRow.getAs("organization_level"),
        joinedRow.getAs("type_of_representation"),
        joinedRow.getAs("gender"),
        1,
        joinedRow.getAs("as_of_date"),
        null,
        "Y"
      )

      return Array(r)

    } else if (joinedRow.getAs("last_name") == null &&
               joinedRow.getAs("first_name") == null &&
               joinedRow.getAs("home_organization") == null) {
      // employee doesn't exist in the incoming data must no longer be employed
      // close the record out

      r = Row(
        joinedRow.getAs("key_d"),
        joinedRow.getAs("last_name_d"),
        joinedRow.getAs("first_name_d"),
        joinedRow.getAs("pay_rate_type_d"),
        joinedRow.getAs("pay_rate_d"),
        joinedRow.getAs("title_description_d"),
        joinedRow.getAs("home_organization_d"),
        joinedRow.getAs("home_organization_description_d"),
        joinedRow.getAs("organization_level_d"),
        joinedRow.getAs("type_of_representation_d"),
        joinedRow.getAs("gender_d"),
        joinedRow.getAs("version_d"),
        joinedRow.getAs("begin_date_d"),
        if (joinedRow.getAs("end_date_d") == null)  as_of_date_str else joinedRow.getAs("end_date_d"), //"2014-11-26 00:00:00",
        "Y")

      return Array(r)

    } else {
      // there was a matching recording in the incoming data
      // compare field by field to see if there is an update
      // if there is we have to close out the existing record
      // and create a new one

      if (joinedRow.getAs("pay_rate_type_d").equals(joinedRow.getAs("pay_rate_type")) &&
          joinedRow.getAs("pay_rate_d").equals(joinedRow.getAs("pay_rate")) &&
          joinedRow.getAs("pay_rate_type_d").equals(joinedRow.getAs("pay_rate_type")) &&
          joinedRow.getAs("title_description_d").equals(joinedRow.getAs("title_description")) &&
          joinedRow.getAs("home_organization_description_d").equals(joinedRow.getAs("home_organization_description")) &&
          joinedRow.getAs("organization_level_d").equals(joinedRow.getAs("organization_level")) &&
          joinedRow.getAs("type_of_representation_d").equals(joinedRow.getAs("type_of_representation")) &&
          joinedRow.getAs("gender_d").equals(joinedRow.getAs("gender"))) {
        // all the fields were the same return the existing _d recording

        r = Row(
          joinedRow.getAs("key_d"),
          joinedRow.getAs("last_name_d"),
          joinedRow.getAs("first_name_d"),
          joinedRow.getAs("pay_rate_type_d"),
          joinedRow.getAs("pay_rate_d"),
          joinedRow.getAs("title_description_d"),
          joinedRow.getAs("home_organization_d"),
          joinedRow.getAs("home_organization_description_d"),
          joinedRow.getAs("organization_level_d"),
          joinedRow.getAs("type_of_representation_d"),
          joinedRow.getAs("gender_d"),
          joinedRow.getAs("version_d"),
          joinedRow.getAs("begin_date_d"),
          as_of_date_str,  //this is the only difference from the existing dim record, probably have to think about how to handle this
          "Y") //hard coding to Y because only Y's get passed into this routine

        return Array(r)

      } else {
        // something was different close the _d record and create a new one

        r = Row(
          joinedRow.getAs("key_d"),
          joinedRow.getAs("last_name_d"),
          joinedRow.getAs("first_name_d"),
          joinedRow.getAs("pay_rate_type_d"),
          joinedRow.getAs("pay_rate_d"),
          joinedRow.getAs("title_description_d"),
          joinedRow.getAs("home_organization_d"),
          joinedRow.getAs("home_organization_description_d"),
          joinedRow.getAs("organization_level_d"),
          joinedRow.getAs("type_of_representation_d"),
          joinedRow.getAs("gender_d"),
          joinedRow.getAs("version_d"),
          joinedRow.getAs("begin_date_d"),
          joinedRow.getAs("as_of_date"),
          "N")

        new_r = Row(
          null,
          joinedRow.getAs("last_name"),
          joinedRow.getAs("first_name"),
          joinedRow.getAs("pay_rate_type"),
          joinedRow.getAs("pay_rate"),
          joinedRow.getAs("title_description"),
          joinedRow.getAs("home_organization"),
          joinedRow.getAs("home_organization_description"),
          joinedRow.getAs("organization_level"),
          joinedRow.getAs("type_of_representation"),
          joinedRow.getAs("gender"),
          joinedRow.getAs("version_d").asInstanceOf[Int] + 1,
          joinedRow.getAs("as_of_date"),
          null,
          "Y"
        )

        return Array(r, new_r)
      }
    }
  }
}

/*
key                 	int
  last_name           	string
  first_name          	string
  pay_rate_type       	string
  pay_rate            	string
  title_description   	string
  home_organization   	string
  home_organization_description	string
  organization_level  	string
  type_of_representation	string
  gender              	string
  version             	int
  begin_date          	string
  end_date            	string
  most_recent         	string
*/