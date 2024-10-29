package io.ilyamor.ks.utils

import org.apache.kafka.common.metrics.Sensor

import java.lang.System.currentTimeMillis

object MetricUtils {

  implicit class MetricsTimer[A](block: => A) {
    def time(sensor: Sensor): A = {
      val start = currentTimeMillis()
      val res = block
      val end = currentTimeMillis()
      sensor.record(end - start, end)
      res
    }

  }
}
