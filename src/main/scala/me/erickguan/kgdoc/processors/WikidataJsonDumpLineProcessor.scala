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

}
