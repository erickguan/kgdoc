package me.erickguan.kgdoc.processors

import java.util.NoSuchElementException

final case class WikiSiteItem(name: String,
                              url: String,
                              dbname: String,
                              code: String,
                              lang: String)

object WikiSite {
  import kantan.csv._
  import kantan.csv.ops._

  implicit val wikiSiteItemDecoder: HeaderDecoder[WikiSiteItem] =
    HeaderDecoder.decoder("name", "url", "dbname", "code", "lang")(
      WikiSiteItem.apply)
  private lazy val wikisites = getClass
    .getResource("/generated/wikisites.csv")
    .asUnsafeCsvReader[WikiSiteItem](rfc.withHeader)
    .toSeq
  private lazy val wikisitesByLanguage = wikisites.groupBy(_.lang)
  private lazy val wikisitesBySite = wikisites.groupBy(_.dbname).map { case (k,v) => (k,v.head)}

  def byLanguage(lang: String): Seq[WikiSiteItem] = {
    try {
      wikisitesByLanguage(lang)
    } catch {
      case _: NoSuchElementException => Seq()
    }
  }

  def url(site: String, title: String): Iterable[String] = {
    val s = wikisitesBySite(site)
    if (s.lang == "zh") {
      List(s"${s.url}/zh-cn/$title", s"${s.url}/zh-tw/$title")
    } else {
      List(s"${s.url}/wiki/$title")
    }
  }
}
