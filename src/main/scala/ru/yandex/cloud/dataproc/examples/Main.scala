package ru.yandex.cloud.dataproc.examples

import com.uber.h3core.H3Core
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.functions.{avg, col, last, lit, to_date, udf, conv}

object Main {
  def main(args: Array[String]) {
    if (args.length != 3 && args.length != 5) { //проверяем аргументы
      System.err.println("Usage spark-app.jar <date:yyyy-mm-dd> <export_to_s3:boolean> <export_to_ch:boolean> or \n" +
        "\tspark-app.jar <date:yyyy-mm-dd> <export_to_s3:boolean> <export_to_ch:boolean> <ch_host:string> <ch_password:string>")
      System.exit(-1)
    }
    val date = args(0) // Дата обработки
    val is_export_to_s3 = args(1).toBoolean // Экспортировать данные в S3
    val is_export_to_ch = args(2).toBoolean // Экспортировать данные в Clickhouse
    var ch_host = ""
    var ch_password = ""
    if (args.length > 3) {
      ch_host = args(3)
      ch_password = args(4)
    }
    val repository = "https://archive.sensor.community" // Архив с данными
    val threads = 64 // Число потоков скачивания с архивы
    val ch_partitions = 16 // Число потоков выгрузки данных в Clickhouse
    val session = SparkSession.builder().getOrCreate()

    /*
     * Получаем списк всех SDS011 сенсоров для указанной даты и создаем RDD с <threads> slices.
     * Для списка сенсоров вызываем метод, который скачивает файлы и создает List<Row> для каждого сенсора.
     * Создаем новый DataFrame по данным от всех сенсоров
     */
      val sds011 = new SDS011Sensor()
      val sds011_listing = session.sparkContext.parallelize(sds011.list(f"${repository}/${date}/"), threads)
      val sds011_data = sds011_listing.map(url => (new SDS011Sensor).create(f"${repository}/${date}/${url}"))
      val df_sds011 = session.createDataFrame(sds011_data.flatMap(list => list), sds011.schema).toDF()
        .withColumn("ts", col("timestamp").cast(TimestampType))
        .drop("timestamp")
      df_sds011.persist()

    // Аналогично строим DataFrame для DHT22
    val dht22 = new DHT22Sensor()
    val dht22_listing = session.sparkContext.parallelize(dht22.list(f"${repository}/${date}/"), threads)
    val dht22_data = dht22_listing.map(url => (new DHT22Sensor).create(f"${repository}/${date}/${url}"))
    val df_dht22 = session.createDataFrame(dht22_data.flatMap(list => list), dht22.schema).toDF()
      .withColumn("ts", col("timestamp").cast(TimestampType))
      .drop("timestamp")
    df_dht22.persist()

    // Аналогично строим DataFrame для BME280
    val bme280 = new BME280Sensor()
    val bme280_listing = session.sparkContext.parallelize(bme280.list(f"${repository}/${date}/"), threads)
    val bme280_data = bme280_listing.map(url => (new BME280Sensor).create(f"${repository}/${date}/${url}"))
    val df_bme280 = session.createDataFrame(bme280_data.flatMap(list => list), bme280.schema).toDF()
      .withColumn("ts", col("timestamp").cast(TimestampType))
      .drop("timestamp")
    df_bme280.persist()

    // Строим DataFrame, который склеивает 3 DF в один по 5минутным интервалам
    val df_5min = create_5min_aggregates(session, df_dht22, df_bme280, df_sds011)
    df_5min.cache()

    // Строим DataFrame, который склеивает 3 DF в один по 20-минутным интервалам
    val df_20min = create_20min_aggregates(session, df_dht22, df_bme280, df_sds011)
    df_20min.cache()

    // Выгружаем в s3, если указана опция
    if(is_export_to_s3) {
      export_to_s3(df_5min, df_20min, date)
    }
    // Выгружаем в ch, если указана опция
    if(is_export_to_ch) {
      export_to_ch(df_5min, df_20min, ch_host, ch_password, ch_partitions)
    }
  }

  def export_to_s3(df_5min: DataFrame, df_20min: DataFrame, date: String) {
    // coalesce(1) указываем чтобы parquet был разбит только на 1 slice, т.к. дневных данных немного
    df_5min.coalesce(1)
    .write.option ("compression", "gzip")
    .mode ("overwrite")
    .parquet (s"s3a://dataproc-breathe/daily/${date}/archive_5min_stations.parquet")

    df_20min
    .coalesce (1)
    .write.option ("compression", "gzip")
    .mode ("overwrite")
    .parquet (s"s3a://dataproc-breathe/daily/${date}/archive_20min_h3p6.parquet")
  }

  def export_to_ch(df_5min: DataFrame, df_20min: DataFrame, host: String, password: String, partitions: Int) {
    df_5min
      .drop("date")
      .write
      .format("jdbc")
      .mode("append")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", f"jdbc:clickhouse://${host}:8443/breathe?ssl=1&sslmode=strict&sslrootcert=/srv/CA.pem")
      .option("dbtable", "point_5min_avg")
      .option("user", "breathe")
      .option("password", password)
      .option("numPartitions", partitions)
      .option("batchsize", 10000)
      .save()

    df_20min
      .withColumn("p6", conv(col("polygon6_id"), 16, 10))
      .drop("polygon6_id")
      .withColumn("polygon6_id", col("p6"))
      .drop("p6")
      .drop("date")
      .write
      .format("jdbc")
      .mode("append")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", f"jdbc:clickhouse://${host}:8443/breathe?ssl=1&sslmode=strict&sslrootcert=/srv/CA.pem")
      .option("dbtable", "area6_20min_median")
      .option("user", "breathe")
      .option("password", password)
      .option("numPartitions", partitions)
      .option("batchsize", 10000)
      .save()
  }

  def create_5min_aggregates(sparkSession: SparkSession, dht22: DataFrame, bme280: DataFrame, sds011: DataFrame): DataFrame = {
    // Делаем агрегаты по дневным данным так же, как делали это по полным данным
    val df_climate = bme280
      .withColumn("time_interval", ((((col("ts").cast(LongType)/300)).cast(LongType))*300).cast(TimestampType))
      .select("sensor_id", "location", "lat", "lon", "temperature", "humidity", "pressure", "time_interval")
      .union(
        dht22
          .withColumn("time_interval", ((((col("ts").cast(LongType)/300)).cast(LongType))*300).cast(TimestampType))
          .withColumn("pressure", lit(null))
          .select("sensor_id", "location", "lat", "lon", "temperature", "humidity", "pressure", "time_interval")
      )
      .groupBy("time_interval", "sensor_id")
      .agg(avg("temperature").as("temperature"), avg("humidity").as("humidity"), last("pressure").as("pressure"), last("location").as("location"))
      .drop("sensor_id")

    var df_sds011 = sds011
      .withColumn("time_interval", ((((col("ts").cast(LongType)/300)).cast(LongType))*300).cast(TimestampType))
      .select("sensor_id", "location", "lat", "lon", "P1", "P2", "time_interval")
      .groupBy("time_interval", "sensor_id")
      .agg(avg("P1").as("P1"), avg("P2").as("P2"), last("lat").as("lat"), last("lon").as("lon"), last("location").as("location"))

    val df_merged = df_climate.join(df_sds011, Seq("time_interval", "location"))
      .select(col("location"), col("lat"), col("lon"), col("time_interval").as("timestamp"), col("P1"), col("P2"), col("temperature"), col("humidity"), col("pressure"))
      .withColumn("date", to_date(col("timestamp")))
      .orderBy("date", "timestamp", "location")
    return df_merged
  }

  @transient lazy val h3 = new ThreadLocal[H3Core] {
    override def initialValue() = H3Core.newInstance()
  }

  def convertToH3Address(xLong: Double, yLat: Double, precision: Int) = {
    h3.get.geoToH3Address(yLat, xLong, precision)
  }

  def create_20min_aggregates(sparkSession: SparkSession, dht22: DataFrame, bme280: DataFrame, sds011: DataFrame): DataFrame = {
    val geoToH3Address: UserDefinedFunction = udf(convertToH3Address _)
    val df_sds011 = sds011
      .withColumn("time_interval", ((((col("ts").cast(LongType)/1200)).cast(LongType))*1200).cast(TimestampType))
      .withColumn("polygon6_id", geoToH3Address(col("lon"), col("lat"), lit(6)))
      .createOrReplaceTempView("df_sds011")

    val df_sds011_filter = sparkSession.sqlContext.sql(
      "select time_interval as timestamp, polygon6_id, percentile_approx(P1,0.5) as P1, percentile_approx(P2,0.5) as P2, count(P1) as dust_measures from df_sds011 group by time_interval, polygon6_id")
      .toDF()

    val df_dht22 = dht22
      .withColumn("time_interval", ((((col("ts").cast(LongType)/1200)).cast(LongType))*1200).cast(TimestampType))
      .withColumn("polygon6_id", geoToH3Address(col("lon"), col("lat"), lit(6)))
      .withColumn("pressure", lit(null))
      .select("polygon6_id", "pressure", "temperature", "humidity", "time_interval")

    val df_bme280 = bme280
      .withColumn("time_interval", ((((col("ts").cast(LongType)/1200)).cast(LongType))*1200).cast(TimestampType))
      .withColumn("polygon6_id", geoToH3Address(col("lon"), col("lat"), lit(6)))
      .select("polygon6_id", "pressure", "temperature", "humidity", "time_interval")

    var df_climate_filter = df_dht22.union(df_bme280)
      .createOrReplaceTempView("df_climate_filter")

    val df_climate = sparkSession.sqlContext.sql(
      "select time_interval as timestamp, polygon6_id, percentile_approx(pressure, 0.5) as pressure, percentile_approx(temperature,0.5) as temperature, percentile_approx(humidity,0.5) as humidity, count(humidity) as climate_measures from df_climate_filter group by time_interval, polygon6_id")

    val df = df_sds011_filter.join(df_climate, Seq("timestamp", "polygon6_id"), "outer")
      .select(col("timestamp"), col("polygon6_id"), col("P1"), col("P2"), col("temperature"), col("humidity"), col("pressure"), col("dust_measures"), col("climate_measures"))
      .withColumn("date", to_date(col("timestamp")))
      .orderBy("timestamp", "polygon6_id")

    return df;
  }
}
