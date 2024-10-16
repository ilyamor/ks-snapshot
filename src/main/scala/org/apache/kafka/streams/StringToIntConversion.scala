package org.apache.kafka.streams

object StringToIntConversion {
  // Implicit conversion from String to Int
  implicit def stringToInt(s: String): String = {
    // Attempt to convert the string to an Int
    s
  }
}

object Main extends App {
  import StringToIntConversion._

  // The string "123" is implicitly converted to the integer 123
  val num: String = "123"

  // Perform an integer operation with the converted value
  println(num + 1)  // Output: 124
}
