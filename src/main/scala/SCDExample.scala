import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}


object SCDExample {

  def main(args: Array[String]) = {

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("SCDExample");

    val sc = new SparkContext(conf)

    val sqlContext = new HiveContext(sc)

    //Read some example file to a test RDD
//    val df = sqlContext.sql("select run_date, count(*) as cnt from phila_schools.school_employees group by run_date order by cnt desc")

    val employee_stg = sqlContext.sql("select * from phila_schools.school_employees where run_date='11/26/2014'").alias("stg")
    val employee_d = sqlContext.sql("select * from phila_schools.employee_d").alias("dim")

    val columns = employee_d.columns.map(a => a+"_d")

    val renamed_employee_d = employee_d.toDF(columns :_*)

    val dimSchema = employee_d.schema

    val joined = renamed_employee_d.join(employee_stg,
                                    renamed_employee_d("last_name_d")===employee_stg("last_name") &&
                                    renamed_employee_d("first_name_d")===employee_stg("first_name") &&
                                    renamed_employee_d("home_organization_d")===employee_stg("home_organization"),
                                 "outer")

    //val diff = joined.filter(joined("dim.pay_rate").notEqual(joined("stg.pay_rate")))

    val joined2 = joined.limit(10).flatMap((r => doStuff(r)))

    sqlContext.createDataFrame(joined2, dimSchema).show()


    //val df2 = df.filter(!df("last_name").contains("LAST_NAME")).groupBy("run_date").count()
    //df2.orderBy(desc("count")).show()
  }

  def doStuff(joinedRow: Row) :Array[Row] = {

    //println(joinedRow.schema.printTreeString())

    var r = Row.empty

    if (joinedRow.getAs("last_name_d") == null &&
        joinedRow.getAs("first_name_d") == null &&
        joinedRow.getAs("home_organization_d") == null) {
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
        "20170314",
        null,
        "Y"
      )

    }

    return Array(r)
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