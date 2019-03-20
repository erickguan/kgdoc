package me.erickguan.kgdoc.extractors

import me.erickguan.kgdoc.json._

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

object WikidataExtractor {
  case class Triple(subject: String, predicate: String, `object`: String)
  case class ItemLangLiteral(item: String, literal: String, lang: String)
  type SiteLink = me.erickguan.kgdoc.json.SiteLink

  def filterNonItem(line: String): Boolean = line != "[" && line != "]"

  private val extractEntityFromDataValue: PartialFunction[DataValue, String] = {
    case WikibaseEntityIdDataValue(_, numericId) => s"Q$numericId"
  }

  def extractTriples(item: WikidataItem): Iterable[Triple] = {
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

  def extractAliases(item: WikidataItem): Iterable[ItemLangLiteral] = {
    for {
      a <- item.aliases
      langItem <- a._2
    } yield ItemLangLiteral(item.id, langItem.value, langItem.language)
  }

  def extractLabels(item: WikidataItem): Iterable[ItemLangLiteral] = {
    for {
      l <- item.labels
      langItem = l._2
    } yield ItemLangLiteral(item.id, langItem.value, langItem.language)
  }

  def extractLabelLiterals(item: WikidataItem): Iterable[ItemLangLiteral] = {
    extractLabels(item) ++ extractAliases(item)
  }

  def extractDescriptions(item: WikidataItem): Iterable[ItemLangLiteral] = {
    for {
      d <- item.descriptions
      langItem = d._2
    } yield ItemLangLiteral(item.id, langItem.value, langItem.language)
  }

  def extractSitelinks(item: WikidataItem): Iterable[SiteLink] = {
    val m = item.sitelinks.getOrElse(Map())
    for {
      (_, siteLink) <- m
    } yield siteLink
  }
}
