package me.erickguan.kgdoc.processors

object DatasetLineProcessor {
  def decodeLine(line: String,
                 separator: Char = '\t'): (String, String, String) = {
    line.split(separator) match {
      case Array(s, p, o) => (s, p, o)
    }
  }
}
