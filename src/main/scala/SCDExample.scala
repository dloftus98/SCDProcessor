import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}


object SCDExample {

  def main(args: Array[String]) = {

    // Start the Spark context
    val conf = new SparkConf()
      .setAppName("SCDExample");

    val sc = new SparkContext(conf)

    val sqlContext = new HiveContext(sc)

    // Read some example file to a test RDD
    // val df = sqlContext.sql("select run_date, count(*) as cnt from phila_schools.school_employees group by run_date order by cnt desc")

    val employee_stg = sqlContext.sql("select * from phila_schools.employees where run_date='11/26/2014'")
    val employee_d_tmp = sqlContext.sql("select *, from_unixtime(unix_timestamp(run_date, 'MM/dd/yyyy')) as as_of_date from phila_schools.employee_d")

    val employee_d_most_recent = employee_d_tmp.filter(employee_d_tmp("most_recent") === "Y")
    val employee_d_old_recs = employee_d_tmp.filter(employee_d_tmp("most_recent") === "N")

    // process most recent dim records and new incoming records
    val columns = employee_d_most_recent.columns.map(a => a+"_d")

    //val columns = employee_d_most_recent.columns.map(a => a+"_d")

    val renamed_employee_d = employee_d.toDF(columns :_*)

    val dimSchema = employee_d.schema

    val joined = renamed_employee_d.join(employee_stg,
                                    renamed_employee_d("last_name_d")===employee_stg("last_name") &&
                                    renamed_employee_d("first_name_d")===employee_stg("first_name") &&
                                    renamed_employee_d("home_organization_d")===employee_stg("home_organization"),
                                 "outer")

    // val diff = joined.filter(joined("dim.pay_rate").notEqual(joined("stg.pay_rate")))

    val joined2 = joined.flatMap((r => doStuff(r)))

    val new_dim = sqlContext.createDataFrame(joined2, dimSchema)

    new_dim.write.mode("overwrite").saveAsTable("phila_schools.temp_table")

    // val df2 = df.filter(!df("last_name").contains("LAST_NAME")).groupBy("run_date").count()
    // df2.orderBy(desc("count")).show()
  }

  def doStuff(joinedRow: Row) :Array[Row] = {

    // println(joinedRow.schema.printTreeString())

    var r = Row.empty
    var new_r = Row.empty

    if (joinedRow.getAs("last_name_d") == null &&
        joinedRow.getAs("first_name_d") == null &&
        joinedRow.getAs("home_organization_d") == null) {
      // new employee that didn't exist in the dim
      r = Row(69,
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
        "2014-11-26 00:00:00",
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
        "2014-11-26 00:00:00",
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
          null,
          "Y")

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
          "2014-11-26 00:00:00",
          "N")

        new_r = Row(joinedRow.getAs("key_d").asInstanceOf[Int] + 1,
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
          "2014-11-26 00:00:00",
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