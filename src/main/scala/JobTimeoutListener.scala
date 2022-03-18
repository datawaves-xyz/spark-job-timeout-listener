package io.iftech

import io.netty.util.{HashedWheelTimer, Timeout}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart}

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.util.Properties.envOrElse

class JobTimeoutListener extends SparkListener with Logging{
  private val jobTimeout = Duration.create(envOrElse("KERNEL_SPARK_JOB_TIMEOUT", "600").toInt, TimeUnit.SECONDS)
  private val timer = new HashedWheelTimer(100, TimeUnit.MILLISECONDS)
  private val timeouts = new mutable.HashMap[Int, Timeout]()
  logInfo(s"JobTimeoutListener is initialized with job timeout ${jobTimeout.toSeconds}")

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val jobId = jobStart.jobId
    val timeout = timer.newTimeout((_: Timeout) => {
      logInfo(s"Job $jobId is running too long (> ${jobTimeout.toSeconds} sec). Gonna be killed")
      try {
        SparkContext.getOrCreate().cancelJob(jobId)
      } catch {
        case e: Error => logError("Getting SparkContext failed", e)
      }
      timeouts -= jobId
    }, jobTimeout.toMillis, TimeUnit.MILLISECONDS)
    timeouts += (jobId -> timeout)
    logInfo(s"JobTimeoutListener is monitoring job $jobId")
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    timeouts -= jobEnd.jobId
  }
}
