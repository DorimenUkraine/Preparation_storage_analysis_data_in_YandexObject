package ru.yandex.cloud.dataproc.examples

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

import scala.util.matching.Regex
import scala.util.matching.Regex.Match

class SDS011Sensor {
  /*
   * Описываем схему данных датчиков SDS011
   */
  val schema = new StructType()
    .add(StructField("sensor_id", LongType, false))
    .add(StructField("sensor_type", StringType, false))
    .add(StructField("location", LongType, false))
    .add(StructField("lat", DoubleType, false))
    .add(StructField("lon", DoubleType, false))
    .add(StructField("timestamp", StringType, false))
    .add(StructField("P1", DoubleType, true))
    .add(StructField("durP1", DoubleType, true))
    .add(StructField("ratioP1", DoubleType, true))
    .add(StructField("P2", DoubleType, true))
    .add(StructField("durP2", DoubleType, true))
    .add(StructField("ratioP2", DoubleType, true))

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
      val p1  = if (cols(6) == "" || cols(6) == "nan") null else cols(6).toDouble
      val dur_p1  = if (cols(7) == "" || cols(7) == "nan") null else cols(7).toDouble
      val ratio_p1  = if (cols(8) == "" || cols(8) == "nan") null else cols(8).toDouble
      val p2  = if (cols(9) == "" || cols(9) == "nan") null else cols(9).toDouble
      val dur_p2  = if (cols(10) == "" || cols(10) == "nan") null else cols(10).toDouble
      val ratio_p2  = if (cols(11) == "" || cols(11) == "nan") null else cols(11).toDouble

      if (Seq(sensor_id, sensor_type, location, lat, lon, timestamp).contains(null)) {
        return Seq()
      }
      return Seq(sensor_id, sensor_type, location, lat, lon, timestamp, p1, dur_p1, ratio_p1, p2, dur_p2, ratio_p2)
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
    val keyValPattern: Regex = ">([0-9\\-]+_sds011_sensor_[0-9]+.csv)<".r
    return keyValPattern.findAllMatchIn(res).toList.map((x: Match) => x.group(1))
  }
}
