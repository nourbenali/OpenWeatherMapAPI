import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.json4s._
import org.json4s.native.JsonMethods._
import org.apache.flink.streaming.api.windowing.time._
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import java.io.FileWriter
import java.net.{HttpURLConnection, URL, URLEncoder}
import scala.util.{Failure, Success, Try}

// SourceFunction to fetch weather data for multiple cities
class OpenWeatherMapSource extends SourceFunction[(String, Double, Double, Double, Double, Double)] {

  private val cities = List("London", "New York", "Paris", "Sydney", "Tokyo", "Dubai", "Moscow")
  private val apiKey = "9ad42e4331030fd4c17744170c84d008"
  private val units = "metric"
  @volatile private var running = true

  override def run(ctx: SourceFunction.SourceContext[(String, Double, Double, Double, Double, Double)]): Unit = {
    while (running) {
      cities.foreach { city =>
        fetchWeatherData(city) match {
          case Success(data) => ctx.collect(data)
          case Failure(ex) => ex.printStackTrace()
        }
      }
      Thread.sleep(300000)
    }
  }

  private def fetchWeatherData(city: String): Try[(String, Double, Double, Double, Double, Double)] = {
    Try {
      implicit val formats: DefaultFormats.type = DefaultFormats
      val encodedCity = URLEncoder.encode(city, "UTF-8")
      val url = s"https://api.openweathermap.org/data/2.5/weather?q=$encodedCity&appid=$apiKey&units=$units"
      val connection = new URL(url).openConnection.asInstanceOf[HttpURLConnection]
      connection.setRequestMethod("GET")

      if (connection.getResponseCode == HttpURLConnection.HTTP_OK) {
        val response = scala.io.Source.fromInputStream(connection.getInputStream).mkString
        val json = parse(response)

        val temperature = (json \ "main" \ "temp").extract[Double]
        val humidity = (json \ "main" \ "humidity").extract[Double]
        val pressure = (json \ "main" \ "pressure").extract[Double]
        val windSpeed = (json \ "wind" \ "speed").extract[Double]
        val clouds = (json \ "clouds" \ "all").extract[Double]

        (city, temperature, humidity, pressure, windSpeed, clouds)
      } else {
        throw new RuntimeException(s"Failed to fetch data for $city with code: ${connection.getResponseCode}")
      }
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}

// SinkFunction to write weather data to a CSV file
class CsvSinkFunction(filePath: String) extends SinkFunction[(String, Double, Double, Double, Double, Double)] {
  override def invoke(value: (String, Double, Double, Double, Double, Double)): Unit = {
    val writer = new FileWriter(filePath, true)
    try {
      writer.write(s"${value._1},${value._2},${value._3},${value._4},${value._5},${value._6}\n")
    } finally {
      writer.close()
    }
  }
}

// Main object to execute the Flink job
object WeatherData {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val weatherDataStream = env.addSource(new OpenWeatherMapSource)

    // Add sliding window to the data stream
    val windowedStream = weatherDataStream
      .keyBy(_._1) // Key by city name for windowing
      .window(SlidingProcessingTimeWindows.of(Time.minutes(15), Time.minutes(5))) // Sliding window of 15 minutes with a slide of 5 minutes
      .reduce((a, b) => (a._1, (a._2 + b._2) / 2, (a._3 + b._3) / 2, (a._4 + b._4) / 2, (a._5 + b._5) / 2, (a._6 + b._6) / 2)) // Average values

    // Define the path where you want to save the CSV file
    val outputPath = "output//weather_data.csv"

    // Add the windowed stream to the sink
    windowedStream.addSink(new CsvSinkFunction(outputPath))

    env.execute("Weather Data Streaming with Sliding Window")
  }
}
