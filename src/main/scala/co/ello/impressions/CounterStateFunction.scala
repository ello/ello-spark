package co.ello.impressions

import org.apache.spark.streaming._

object CounterStateFunction {
  def trackStateFunc(batchTime: Time, key: String, value: Option[Long], state: State[Long]): Option[(String, Long)] = {
    val sum = value.getOrElse(0L) + state.getOption.getOrElse(0L)
    val output = (key, sum)
    state.update(sum)
    Some(output)
  }
}
