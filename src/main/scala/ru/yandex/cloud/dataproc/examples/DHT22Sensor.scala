package ru.yandex.cloud.dataproc.examples

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

import scala.util.matching.Regex
import scala.util.matching.Regex.Match

class DHT22Sensor {
  /*
   * Описываем схему данных датчиков DHT22
   */
  val schema = new StructType()
    .add(StructField("sensor_id", LongType, false))
    .add(StructField("sensor_type", StringType, false))
    .add(StructField("location", LongType, false))
    .add(StructField("lat", DoubleType, false))
    .add(StructField("lon", DoubleType, false))
    .add(StructField("timestamp", StringType, false))
    .add(StructField("temperature", DoubleType, true))
    .add(StructField("humidity", DoubleType, true))

  protected def parse(row: String): Seq[Any] = {
    /*
     * В этом методе мы вручную парсим csv строку, для того чтобы привести колонки к нужным типам.
     * Делаем фильтрацию по данным, у которых вместе обязательных колонок есть NULL.
     */
    val cols = row.split(";", -1)
    try {
      val sensor_id = if (cols(0) == "" || cols(0) == "nan") null else cols(0).toLong
      val sensor_type = if (cols(1) == "" || cols(1) == "nan") null else cols(1).toString
      val location = if (cols(2) == "" || cols(2) == "nan") null else cols(2).toLong
      val lat = if (cols(3) == "" || cols(3) == "nan") null else cols(3).toDouble
      val lon = if (cols(4) == "" || cols(4) == "nan") null else cols(4).toDouble
      val timestamp = if (cols(5) == "" || cols(5) == "nan") null else cols(5).toString
      val temperature  = if (cols(6) == "" || cols(6) == "nan") null else cols(6).toDouble
      val humidity = if (cols(7) == "" || cols(7) == "nan") null else cols(7).toDouble

      if (Seq(sensor_id, sensor_type, location, lat, lon, timestamp).contains(null)) {
        return Seq()
      }
      return Seq(sensor_id, sensor_type, location, lat, lon, timestamp, temperature, humidity)
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
    val keyValPattern: Regex = ">([0-9\\-]+_dht22_sensor_[0-9]+.csv)<".r
    return keyValPattern.findAllMatchIn(res).toList.map((x: Match) => x.group(1))
  }
}

