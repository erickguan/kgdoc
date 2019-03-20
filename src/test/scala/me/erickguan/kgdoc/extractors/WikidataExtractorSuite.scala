package me.erickguan.kgdoc.extractors

import me.erickguan.kgdoc.KgdocFixtureSuite
import me.erickguan.kgdoc.json.{SiteLink, WikidataItem}
import org.scalatest.{Outcome, Tag}

import scala.io.Source

class WikidataExtractorSuite extends KgdocFixtureSuite {
  import WikidataExtractor._

  case class FixtureParam(items: Seq[WikidataItem])

  def withFixture(test: OneArgTest): Outcome = {
    val source = Source.fromURL(getClass.getResource("/fixtures/wikidata.json"))
    val items = source.getLines
      .filter(filterNonItem)
      .map(WikidataItem.processJsonLine)
      .toSeq
    val fixtureParam = FixtureParam(items)
    try withFixture(test.toNoArgTest(fixtureParam)) // Invoke the test function
    finally {
      // Perform cleanup
    }
  }

  test("can extract triples", Tag("extraction")) { f =>
    assert(
      f.items.flatMap(extractTriples) === Seq(
        Triple("Q1", "P31", "Q1454986"),
        Triple("Q23", "P31", "Q5"),
        Triple("Q298", "P31", "Q6256"),
        Triple("Q298", "P31", "Q3624078"),
        Triple("Q3614", "P31", "Q179661")
      ))
  }

  test("can extract labels", Tag("extraction")) { f =>
    assert(
      f.items.flatMap(extractLabelLiterals) === Seq(
        ItemLangLiteral("Q3614", "Occipital nerve stimulation", "en"),
        ItemLangLiteral("Q3614", "стимуляция затылочного нерва", "ru"),
        ItemLangLiteral("Q3614",
                        "Peripheral nerve stimulation of the occipital nerves",
                        "en"),
        ItemLangLiteral("P19", "出生地", "zh"),
        ItemLangLiteral("P19", "出生地", "zh-tw"),
        ItemLangLiteral("P19", "出生地", "zh-hans"),
        ItemLangLiteral("P19", "出生地", "zh-hant"),
        ItemLangLiteral("P19", "place of birth", "en"),
        ItemLangLiteral("P19", "birthplace", "en"),
        ItemLangLiteral("P19", "born in", "en"),
        ItemLangLiteral("P19", "POB", "en"),
        ItemLangLiteral("P19", "birth place", "en"),
        ItemLangLiteral("P19", "location born", "en"),
        ItemLangLiteral("P19", "born at", "en"),
        ItemLangLiteral("P19", "birth location", "en"),
        ItemLangLiteral("P19", "location of birth", "en"),
        ItemLangLiteral("P19", "birth city", "en")
      ))
  }

  test("can extract descriptions", Tag("extraction")) { f =>
    assert(
      f.items.flatMap(extractDescriptions) == Seq(
        ItemLangLiteral("Q3614", "medical treatment", "en"),
        ItemLangLiteral("Q3614", "traitement médical", "fr"),
        ItemLangLiteral(
          "P19",
          "lieu où la personne est née, le plus spécifique possible (exemple : ville plutôt que pays)",
          "fr"),
        ItemLangLiteral(
          "P19",
          "most specific known (e.g. city instead of country, or hospital instead of city) birth location of a person, animal or fictional character",
          "en"
        )
      ))
  }

  test("can extract sitelinks", Tag("extraction")) { f =>
    assert(
      f.items.flatMap(extractSitelinks) == Seq(
        SiteLink("enwiki", "Occipital nerve stimulation"),
        SiteLink("hewiki", "גירוי עצבי עורפי")
      ))
  }
}
