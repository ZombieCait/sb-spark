import scala.io.Source._
import collection.immutable.ListMap

object lab01 {
  val marks = fromFile("ml-100k/u.data").getLines.toList.map(_.split('\t'))
  val grouped_marks = ListMap(marks.groupBy(_(2)).mapValues(_.size).toSeq.sortBy(_._1):_*)
  val grouped_marks_film9 = ListMap(marks.filter(_(1)=="9").groupBy(_(2)).mapValues(_.size).toSeq.sortBy(_._1):_*)
  val result = Map("hist_film"->grouped_marks.values.toList.mkString("[",",","]"),
    "hist_all"->grouped_marks_film9.values.toList.mkString("[",",","]"))
  reflect.io.File("lab01.json").writeAll(JSONObject(result).toString())

}