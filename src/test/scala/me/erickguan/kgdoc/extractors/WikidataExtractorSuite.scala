package me.erickguan.kgdoc.extractors

import me.erickguan.kgdoc.json.{Claim, WikibaseEntityIdDataValue, WikidataItem}

import scala.collection.mutable

case class Triple(subject: String, predicate: String, `object`: String)
case class ItemStringLiteral(id: String, value: String, language: String)

object WikidataExtractor {
  def extractTriples(item: WikidataItem): Seq[Triple] = {
    val triples = mutable.ArrayBuffer[Triple]()
    val s = item.id
    item.claims.foreach((e: (String, List[Claim])) => {
      val p = e._1
      e._2.foreach(claim => {
        if (claim.rank == "preferred" || claim.rank == "normal") {
          val snak = claim.mainsnak
          val datatype = snak.datatype
          if (datatype == "wikibase-item") {
            val datavalue = snak.datavalue.asInstanceOf[WikibaseEntityIdDataValue]
            val o = s"Q${datavalue.numericId}"
            triples += Triple(s, p, o)
          }
        }
      })
    })
    triples
  }

  def extractLabels(item: WikidataItem): Seq[ItemStringLiteral] = {

  }

  def extractDescriptions(item: WikidataItem): Seq[ItemStringLiteral] = {

  }
}
