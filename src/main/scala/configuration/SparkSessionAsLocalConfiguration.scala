package configuration

import org.apache.spark.sql.SparkSession

/**
 * Создаёт объект SparkSession для локальной работы со спарк.
 */
object SparkSessionAsLocalConfiguration {
  lazy val sc: SparkSession = {
    SparkSession.builder().master("local").appName("gabidullinDA").getOrCreate()
  }
}
