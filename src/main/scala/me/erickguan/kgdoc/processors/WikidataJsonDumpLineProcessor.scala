package me.erickguan.kgdoc.processors

import me.erickguan.kgdoc.json.WikidataItem
import me.erickguan.kgdoc.json.WikidataItem.decodeJson

object WikidataJsonDumpLineProcessor {
  def filterNonItem(line: String): Boolean = line != "[" && line != "]"

  private def rstripLine(line: String): String = {
    line.stripSuffix(",")
  }

  def decodeJsonLine(line: String): WikidataItem = {
    decodeJson(rstripLine(line))
  }

  def extract(item: WikidataItem,
              f: WikidataItem => Iterable[LineTransformable],
              separator: Char = '\t'): Iterable[String] = {
    f(item).map(_.toLine(separator))
  }

  def extractJsonLine(line: String,
                      f: WikidataItem => Iterable[LineTransformable],
                      separator: Char = '\t'): Iterable[String] = {
    extract(decodeJsonLine(line), f, separator)
  }

}
