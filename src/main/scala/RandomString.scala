import scala.util.Random

object RandomString {

  def main(args: Array[String]) = {

    var same = 0;
    var start = System.currentTimeMillis()
    var subduration = 0L;

    for (i <- 1 to 1000000) {
      val substart = System.currentTimeMillis()
      val str1 = Random.alphanumeric.take(100).mkString
      var str2 = ""

      if (Random.nextInt(10) <= 2) {
        str2 = Random.alphanumeric.take(100).mkString
      } else {
        str2 = str1
      }
      val subend = System.currentTimeMillis()

      subduration += subend - substart

      if (str1 == str2) {
        same += 1
      }
    }
    var end = System.currentTimeMillis()

    println("same = " + same + " duration = " + (end - start - subduration))

   }
}
