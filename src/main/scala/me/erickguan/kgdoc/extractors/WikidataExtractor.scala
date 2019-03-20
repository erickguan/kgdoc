package me.erickguan.kgdoc.extractors

import me.erickguan.kgdoc.json.WikidataItem.decodeJson
import me.erickguan.kgdoc.json._
import me.erickguan.kgdoc.processors.LineTransformable

object FilterHelpers {
  implicit class WikidataItemCondition(item: WikidataItem) {
    def isEntity: Boolean = item.`type` == "item"
  }

  implicit class ClaimCondition(claim: Claim) {
    def isValid: Boolean = claim.rank == "normal" || claim.rank == "preferred"
  }

  implicit class SnakCondition(snak: Snak) {
    def isEntity: Boolean = snak.datatype == "wikibase-item"
  }
}

case class Triple(subject: String, predicate: String, `object`: String)
case class ItemLangLiteral(item: String, literal: String, lang: String)
case class WikiSiteLink(item: String, site: String, title: String)

object WikidataExtractionLineTransform {
  implicit class TripleLine(t: Triple) extends LineTransformable {
    override def toLine(separator: Char): String =
      s"${t.subject}$separator${t.predicate}$separator${t.`object`}"
  }

  implicit class ItemLangLiteralLine(i: ItemLangLiteral)
      extends LineTransformable {
    override def toLine(separator: Char): String =
      s"${i.item}$separator${i.literal}$separator${i.lang}"
  }

  implicit class WikiSiteLinkLine(l: WikiSiteLink) extends LineTransformable {
    override def toLine(separator: Char): String = {
      s"${l.item}$separator${l.site}$separator${l.title}"
    }
  }
}

object WikidataExtractor {

  private val extractEntityFromDataValue: PartialFunction[DataValue, String] = {
    case WikibaseEntityIdDataValue(_, numericId) => s"Q$numericId"
  }

  def triples(item: WikidataItem): Iterable[Triple] = {
    import FilterHelpers._

    if (item.isEntity) {
      for {
        (p, c) <- item.claims
        claim <- c
        if claim.isValid
        snak = claim.mainsnak
        if snak.isEntity
        dv = snak.datavalue
        if extractEntityFromDataValue.isDefinedAt(dv)
      } yield Triple(item.id, p, extractEntityFromDataValue(dv))
    } else {
      Iterable()
    }
  }

  def aliases(item: WikidataItem): Iterable[ItemLangLiteral] = {
    for {
      a <- item.aliases
      langItem <- a._2
    } yield ItemLangLiteral(item.id, langItem.value, langItem.language)
  }

  def labels(item: WikidataItem): Iterable[ItemLangLiteral] = {
    for {
      l <- item.labels
      langItem = l._2
    } yield ItemLangLiteral(item.id, langItem.value, langItem.language)
  }

  def labelLiterals(item: WikidataItem): Iterable[ItemLangLiteral] = {
    labels(item) ++ aliases(item)
  }

  def descriptions(item: WikidataItem): Iterable[ItemLangLiteral] = {
    for {
      d <- item.descriptions
      langItem = d._2
    } yield ItemLangLiteral(item.id, langItem.value, langItem.language)
  }

  def sitelinks(item: WikidataItem): Iterable[WikiSiteLink] = {
    val m = item.sitelinks.getOrElse(Map())
    for {
      (_, siteLink) <- m
    } yield WikiSiteLink(item.id, siteLink.site, siteLink.title)
  }

}
