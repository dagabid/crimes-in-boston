package service

import org.apache.spark.sql.{Dataset, Row}

trait CSVReader {
  def load(dataPath: String): Dataset[Row]
}
