package service.impl

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import service.BostonCrimeDataMartWriterService

class BostonCrimeDataMartWriterServiceImpl(sc: SparkSession, crimeDF: Dataset[Row], offensiveCodeDF: Dataset[Row], outputPathParquet: String) extends BostonCrimeDataMartWriterService {
  override def make_mart(): Unit = {
    crimeDF.createOrReplaceTempView("crime")
    offensiveCodeDF.createOrReplaceTempView("offense_code_view")

    val sqlFullTable: String =
      """
        with transform_offense_code as (
                    select lpad(code, 5, "0") code,
                           name,
                           split(regexp_replace(name, "\"", ""), ' - ')[0]  crime_type,
                           row_number() over (partition by lpad(code, 5, "0") order by lpad(code, 5, "0")) rn
                    from offense_code_view
             ),
             drop_duplicate_code as (
                    select code, name, crime_type
                    from transform_offense_code
                    where rn = 1
             ),
             transform_crime as (
                    select /*+ BROADCAST(drop_duplicate_code) */
                          year, month, district, incident_number, lpad(OFFENSE_CODE, 5, "0") OFFENSE_CODE, Lat, Long, dc.*
                    from crime c join drop_duplicate_code dc ON lpad(c.OFFENSE_CODE, 5, "0") = dc.code
                    where district is not null
             )

        select *
        from transform_crime
      """
        .stripMargin

    sc.sqlContext.sql(sqlFullTable).createOrReplaceTempView("full_table")

    val sqlTotalAndAVG =
      """
        select                      district,
         count(incident_number) crimes_total,
                                avg(Lat) lat,
                               avg(Long) lng
        from full_table
        group by district
      """.stripMargin

    sc.sqlContext.sql(sqlTotalAndAVG).createOrReplaceTempView("avg_lat_long_table")

    val sqlMedian =
      """
     with prepared_median_select as (
       select district, count(incident_number) crimes_total_by_month_district
       from full_table
       group by month, district
     )
       select district, percentile_approx(crimes_total_by_month_district, 0.5) crimes_monthly
       from prepared_median_select
       group by district
      """.stripMargin
    sc.sqlContext.sql(sqlMedian).createOrReplaceTempView("median")

    val sqlTopFreq =
      """
     with cnt_crime_type as (
       select district, crime_type, count(incident_number) cnt_crime_type
       from full_table
       group by district, crime_type
     ),
     cnt_freq as (
            select cnt_crime_type.*,
            row_number() over(partition by district order by district, cnt_crime_type desc) rn
     from cnt_crime_type
     )
     select district, concat_ws(', ', collect_list(crime_type)) frequent_crime_types
     from cnt_freq
     where rn < 4
     group by district
      """.stripMargin
    sc.sqlContext.sql(sqlTopFreq).createOrReplaceTempView("crime_freq")

    val sqlDataMart =
      """
      select crimes_total, crimes_monthly, frequent_crime_types, lat, lng
      from avg_lat_long_table a
      join median m ON a.district = m.district
      join crime_freq f ON a.district = f.district
      """.stripMargin

    sc.sqlContext.sql(sqlDataMart)
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(outputPathParquet)
  }
}
