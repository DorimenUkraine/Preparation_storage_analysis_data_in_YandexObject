package ru.yandex.cloud.dataproc.examples

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

import scala.util.matching.Regex
import scala.util.matching.Regex.Match

class BME280Sensor {
  /*
   * Описываем схему данных датчиков BME280
   */
  val schema = new StructType()
    .add(StructField("sensor_id", StringType, false))
    .add(StructField("sensor_type", StringType, false))
    .add(StructField("location", LongType, false))
    .add(StructField("lat", DoubleType, false))
    .add(StructField("lon", DoubleType, false))
    .add(StructField("timestamp", StringType, false))
    .add(StructField("pressure", DoubleType, true))
    .add(StructField("altitude", DoubleType, true))
    .add(StructField("pressure_sealevel", DoubleType, true))
    .add(StructField("temperature", DoubleType, true))
    .add(StructField("humidity", DoubleType, true))


  protected def parse(row: String): Seq[Any] = {
    /*
     * В этом методе мы вручную парсим csv строку, для того чтобы привести колонки к нужным типам.
     * Делаем фильтрацию по данным, у которых вместе обязательных колонок есть NULL.
     */
    val cols = row.split(";", -1)
    try {
      val sensor_id = if (cols(0) == "" || cols(0) == "nan") null else cols(0).toString
      val sensor_type = if (cols(1) == "" || cols(1) == "nan") null else cols(1).toString
      val location = if (cols(2) == "" || cols(2) == "nan") null else cols(2).toLong
      val lat = if (cols(3) == "" || cols(3) == "nan") null else cols(3).toDouble
      val lon = if (cols(4) == "" || cols(4) == "nan") null else cols(4).toDouble
      val timestamp = if (cols(5) == "" || cols(5) == "nan") null else cols(5).toString
      val pressure  = if (cols(6) == "" || cols(6) == "nan") null else cols(6).toDouble
      val altitude  = if (cols(7) == "" || cols(7) == "nan") null else cols(7).toDouble
      val pressure_sealevel = if (cols(8) == "" || cols(8) == "nan" ) null else cols(8).toDouble
      val temperature  = if (cols(9) == "" || cols(9) == "nan") null else cols(9).toDouble
      val humidity = if (cols(10) == "" || cols(10) == "nan") null else cols(10).toDouble

      if (Seq(sensor_id, sensor_type, location, lat, lon, timestamp).contains(null)) {
        return Seq()
      }
      return Seq(sensor_id, sensor_type, location, lat, lon, timestamp, pressure, altitude, pressure_sealevel, temperature, humidity)
    }
    catch {
      case e:Exception=>
        println(f"Wrong row: ${row}, exception: ${e}")
        return Seq()
    }
  }

  def create(url: String): List[Row] = {
    val measurements = scala.io.Source.fromURL(url).mkString.stripMargin.lines.toList.tail.toSeq
    return measurements.map(row => parse(row)).filter(_ != Seq()).map(row => Row.fromSeq(row)).toList
  }

  def list(url: String): List[String] = {
    var res = scala.io.Source.fromURL(url).mkString
    val keyValPattern: Regex = ">([0-9\\-]+_bme280_sensor_[0-9]+.csv)<".r
    return keyValPattern.findAllMatchIn(res).toList.map((x: Match) => x.group(1))
  }
}

