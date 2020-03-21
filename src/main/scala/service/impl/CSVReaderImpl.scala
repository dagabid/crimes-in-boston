package service.impl

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import service.CSVReader

class CSVReaderImpl(sc: SparkSession) extends CSVReader {
  override def load(dataPath: String): Dataset[Row] = {
    sc.sqlContext
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(dataPath)
  }
}
