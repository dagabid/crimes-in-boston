import configuration.SparkSessionAsLocalConfiguration
import service.impl.{BostonCrimeDataMartWriterServiceImpl, CSVReaderImpl}

object BostonCrimesMap extends App {
  val csvReader = new CSVReaderImpl(SparkSessionAsLocalConfiguration.sc)
  new BostonCrimeDataMartWriterServiceImpl(
    SparkSessionAsLocalConfiguration.sc,
    csvReader.load(args(0)),
    csvReader.load(args(1)),
    args(2)
  ).make_mart()
}
