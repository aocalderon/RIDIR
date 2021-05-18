import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.sql.Timestamp

object Main extends App {
  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._

  println(args(0))
  val appId = args(0)
  val tasks = spark.read.option("delimiter", "\t")
    .option("header", "true")
    .csv("/home/and/RIDIR/Meetings/next/figures/tasks.tsv")
    .filter(s"appId == 'application_1615435002078_0${appId}'")
    .select($"index", $"host", $"launchTime", $"duration")
  tasks.show(truncate=false)

  val filename = s"/home/and/RIDIR/Meetings/next/figures/tasks/${appId}.tsv"
  val f = new java.io.PrintWriter(filename)
  f.write(
    tasks.map{ row =>
      val taskId = row.getString(0)
      val host = row.getString(1)
      val str = row.getString(2).replace("GMT","").replace("T", " ")
      val launch = Timestamp.valueOf(str).getTime
      val duration = row.getString(3).toLong
      val finish = launch + duration

      s"$taskId\t$host\t$launch\t$finish\t$duration\n"
    }.collect.mkString("")
  )
  f.close

  /*
  val starts = tasks.map{ d =>
    val index = d.getString(0).toInt
    val str = d.getString(2).replace("GMT","").replace("T", " ")
    val stamp = Timestamp.valueOf(str)

    (index, stamp.getTime)
  }.toDF("index2", "millis")

  val data = tasks.join(starts, $"index" === $"index2")
    .select($"index", $"host", $"millis", $"duration")
  val table = data.flatMap{ row =>
    val index = row.getString(0).toInt
    val host  = row.getString(1)
    val start = row.getLong(2)
    val arr   = 0 to row.getString(3).toInt
    val interval  =arr.map(_ + start)

    interval.map( i => (index, host, i))
  }.toDF("index", "host", "milli")//.show(truncate=false)

  table.createTempView("tasks")
  val perf = spark.sql("SELECT milli, count(index), collect_list(host) FROM tasks GROUP BY milli ORDER BY milli")
  //.show(truncate=false)

  import collection.JavaConverters._
  val cores = perf.map{ row =>
    val times = row.getLong(0)
    val cores = row.getLong(1)
    val hosts = row.getList[String](2).asScala.distinct.size

    (times, cores, hosts)
  }.toDF("times","cores","hosts")
  .select(from_unixtime($"times"/1000.0, "yyyy-MM-dd HH:mm:ss").as("timestamp"), $"cores", $"hosts")
  //cores.show(truncate=false)

  val f = new java.io.PrintWriter(s"/tmp/cores/c${appId}.csv")
  f.write(
    cores.groupBy($"timestamp").agg(avg($"cores"), avg($"hosts")).orderBy($"timestamp")
      .map{ row =>
        val timestamp = row.getString(0)
        val cores = row.getDouble(1)

        s"$timestamp\t$cores\n"
      }.collect.mkString("")
  )
  f.close
   */
  spark.close
}
