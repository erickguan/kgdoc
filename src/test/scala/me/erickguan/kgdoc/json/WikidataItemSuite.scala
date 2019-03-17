package me.erickguan.kgdoc.json

import me.erickguan.kgdoc.KgdocSuite
import io.circe.parser.decode
import io.circe.generic.auto._
import org.scalatest.{BeforeAndAfterAll, Tag}
import io.circe.generic.extras._
import io.circe.syntax._

class WikidataItemSuite extends KgdocSuite {
  test("LangItem parsing works", Tag("parsing")) {
    val json = """{
      "language": "en",
      "value": "NYC"
    }"""
    val langItem = decode[LangItem](json)
    assert(langItem === Right(LangItem("en", "NYC")))
  }

  test("SiteLink parsing works", Tag("parsing")) {
    val json = """{
                 |   "site": "dewiki",
                 |   "title": "New York City",
                 |   "badges": [
                 |       "Q17437798"
                 |    ]
                 |  }""".stripMargin
    val siteLink = decode[SiteLink](json)
    assert(siteLink === Right(SiteLink("dewiki", "New York City")))
  }

  test("StringDataValue parsing works", Tag("parsing")) {
    val json = """{"value": "A.jpg"}"""
    val sdv = decode[StringDataValue](json)
    assert(sdv === Right(StringDataValue("A.jpg")))
  }

  test("WikibaseEntityIdDataValue parsing works", Tag("parsing")) {
    val json = """{
                 |                                    "entity-type": "item",
                 |                                    "numeric-id": 328
                 |                            }""".stripMargin
    val wdv = decode[WikibaseEntityIdDataValue](json)
    assert(wdv == Right(WikibaseEntityIdDataValue("item", 328)))
  }

  test("GlobeCoordinateDataValue parsing works", Tag("parsing")) {
    val json = """ {
                 |              "latitude": 52.516666666667,
                 |              "longitude": 13.383333333333,
                 |              "altitude": null,
                 |              "precision": 0.016666666666667,
                 |              "globe": "http:\/\/www.wikidata.org\/entity\/Q2"
                 | } """.stripMargin
    val gcdv = decode[GlobeCoordinateDataValue](json)
    assert(
      gcdv === Right(
        GlobeCoordinateDataValue(52.516666666667,
                                 13.383333333333,
                                 0.016666666666667,
                                 "http://www.wikidata.org/entity/Q2")))
  }

  test("QuantityDataValue parsing works", Tag("parsing")) {
    val json =
      """{
                 |              "amount":"+10.38",
                 |              "upperBound":"+10.375",
                 |              "lowerBound":"+10.385",
                 |              "unit":"http:\/\/www.wikidata.org\/entity\/Q712226"
                 |            }""".stripMargin
    val qdv = decode[QuantityDataValue](json)
    assert(
      qdv === Right(
        QuantityDataValue("+10.38",
                          "+10.375",
                          "+10.385",
                          "http://www.wikidata.org/entity/Q712226")))
  }

  test("TimeDataValue parsing works", Tag("parsing")) {
    val json =
      """{
                 |              "time": "+2001-12-31T00:00:00Z",
                 |              "timezone": 0,
                 |              "before": 0,
                 |              "after": 0,
                 |              "precision": 11,
                 |              "calendarmodel": "http:\/\/www.wikidata.org\/entity\/Q1985727"
                 |   }""".stripMargin
    val tdv = decode[TimeDataValue](json)
    assert(
      tdv === Right(
        TimeDataValue("+2001-12-31T00:00:00Z",
                      0,
                      0,
                      0,
                      11,
                      "http://www.wikidata.org/entity/Q1985727")))
  }

  test("DataValue parsing works", Tag("parsing")) {
    val stringJson = """{
                 |            "value": "SomePicture.jpg",
                 |            "type": "string"
                 |          }""".stripMargin
    val sdv = decode[DataValue](stringJson)
    assert(sdv === Right(StringDataValue("SomePicture.jpg")))

    val gcJson = """{
                   | "value": {
                   |              "latitude": 52.516666666667,
                   |              "longitude": 13.383333333333,
                   |              "altitude": null,
                   |              "precision": 0.016666666666667,
                   |              "globe": "http:\/\/www.wikidata.org\/entity\/Q2"
                   |            },
                   |            "type": "globecoordinate"
                   | }""".stripMargin

    val gcdv = decode[DataValue](gcJson)
    assert(
      gcdv === Right(
        GlobeCoordinateDataValue(52.516666666667,
                                 13.383333333333,
                                 0.016666666666667,
                                 "http://www.wikidata.org/entity/Q2")))

    val qJson = """{"value":{
        |              "amount":"+10.38",
        |              "upperBound":"+10.375",
        |              "lowerBound":"+10.385",
        |              "unit":"http://www.wikidata.org/entity/Q712226"
        |            },
        |            "type":"quantity"
        | }
      """.stripMargin
    val qdv = decode[DataValue](qJson)
    assert(
      qdv === Right(
        QuantityDataValue("+10.38",
                          "+10.375",
                          "+10.385",
                          "http://www.wikidata.org/entity/Q712226")))

    val tJson =
      """{"value": {
                  |              "time": "+2001-12-31T00:00:00Z",
                  |              "timezone": 0,
                  |              "before": 0,
                  |              "after": 0,
                  |              "precision": 11,
                  |              "calendarmodel": "http:\/\/www.wikidata.org\/entity\/Q1985727"
                  |            },
                  |            "type": "time"
                  | }""".stripMargin
    val tdv = decode[DataValue](tJson)
    assert(
      tdv === Right(
        TimeDataValue("+2001-12-31T00:00:00Z",
                      0,
                      0,
                      0,
                      11,
                      "http://www.wikidata.org/entity/Q1985727")))

  }

  test("Snak parsing works", Tag("parsing")) {
    val json = """{
                 |          "snaktype": "value",
                 |          "property": "P356",
                 |          "datatype": "string",
                 |          "datavalue": {
                 |            "value": "SomePicture.jpg",
                 |            "type": "string"
                 |          }
                 |        }""".stripMargin
    val snak = decode[Snak](json)
    assert(
      snak === Right(
        Snak("value", "P356", "string", StringDataValue("SomePicture.jpg"))))
  }

  test("Claim parsing works", Tag("parsing")) {
    val json =
      """{
                 |  "id": "q60$5083E43C-228B-4E3E-B82A-4CB20A22A3FB",
                 |  "mainsnak": {
                 |    "snaktype": "value",
                 |    "property": "P625",
                 |    "datatype": "globecoordinate",
                 |    "datavalue": {
                 |      "value": {
                 |        "latitude": 40.67,
                 |        "longitude": -73.94,
                 |        "altitude": null,
                 |        "precision": 0.00027777777777778,
                 |        "globe": "http://www.wikidata.org/entity/Q2"
                 |      },
                 |      "type": "globecoordinate"
                 |    }
                 |  },
                 |  "type": "statement",
                 |  "rank": "normal",
                 |  "qualifiers": {
                 |    "P580": [
                 |      {
                 |        "hash": "sssde3541cc531fa54adcaffebde6bef28g6hgjd",
                 |        "snaktype": "value",
                 |        "property": "P580",
                 |        "datatype": "time",
                 |        "datavalue": {
                 |          "value": {
                 |            "time": "+00000001994-01-01T00:00:00Z",
                 |            "timezone": 0,
                 |            "before": 0,
                 |            "after": 0,
                 |            "precision": 11,
                 |            "calendarmodel": "http:\/\/www.wikidata.org\/entity\/Q1985727"
                 |          },
                 |          "type": "time"
                 |        }
                 |      }
                 |    ]
                 |  },
                 |  "references": [
                 |    {
                 |      "hash": "d103e3541cc531fa54adcaffebde6bef28d87d32",
                 |      "snaks": []
                 |    }
                 |  ]
                 |}""".stripMargin
    val claim = decode[Claim](json)
    assert(
      claim === Right(Claim(
        "statement",
        Snak("value",
             "P625",
             "globecoordinate",
             GlobeCoordinateDataValue(40.67,
                                      -73.94,
                                      0.00027777777777778,
                                      "http://www.wikidata.org/entity/Q2")),
        "normal",
        Map(
          ("P580",
           List(
             Snak("value",
                  "P580",
                  "time",
                  TimeDataValue("+00000001994-01-01T00:00:00Z",
                                0,
                                0,
                                0,
                                11,
                                "http://www.wikidata.org/entity/Q1985727")))))
      )))
  }

  test("WikidataItem parsing works", Tag("parsing")) {
    val json =
      """{
                 |  "sitelinks": {
                 |    "de": {
                 |      "site": "dewiki",
                 |      "title": "New York City",
                 |      "badges": [
                 |          "Q17437798"
                 |       ]
                 |     }
                 |  },
                 |  "claims": [
                 |    {
                 |    "P582": {
                 |      "id": "q60$5083E43C-228B-4E3E-B82A-4CB20A22A3FB",
                 |      "mainsnak": {
                 |        "snaktype": "value",
                 |        "property": "P625",
                 |        "datatype": "globecoordinate",
                 |        "datavalue": {
                 |          "value": {
                 |            "latitude": 40.67,
                 |            "longitude": -73.94,
                 |            "altitude": null,
                 |            "precision": 0.00027777777777778,
                 |            "globe": "http://www.wikidata.org/entity/Q2"
                 |          },
                 |          "type": "globecoordinate"
                 |        }
                 |      },
                 |      "type": "statement",
                 |      "rank": "normal",
                 |      "qualifiers": {
                 |        "P580": [
                 |          {
                 |            "hash": "sssde3541cc531fa54adcaffebde6bef28g6hgjd",
                 |            "snaktype": "value",
                 |            "property": "P580",
                 |            "datatype": "time",
                 |            "datavalue": {
                 |              "value": {
                 |                "time": "+00000001994-01-01T00:00:00Z",
                 |                "timezone": 0,
                 |                "before": 0,
                 |                "after": 0,
                 |                "precision": 11,
                 |                "calendarmodel": "http:\/\/www.wikidata.org\/entity\/Q1985727"
                 |              },
                 |              "type": "time"
                 |            }
                 |          }
                 |        ]
                 |      },
                 |      "references": [
                 |        {
                 |          "hash": "d103e3541cc531fa54adcaffebde6bef28d87d32",
                 |          "snaks": []
                 |        }
                 |      ]
                 |    }
                 |  }
                 |  ],
                 |  "aliases": {
                 |    "en": [{
                 |
                 |        "language": "en",
                 |        "value": "NYC"
                 |
                 |    }]
                 |  },
                 |  "descriptions": {
                 |    "en": {
                 |      "language": "en",
                 |      "value": "english desc"
                 |    }
                 |  },
                 |  "labels": {
                 |    "en": {
                 |      "language": "en",
                 |      "value": "NYC"
                 |    }
                 |  },
                 |  "type": "item",
                 |  "id": "asde"
                 |}""".stripMargin
    val claim = Claim(
      "statement",
      Snak("value",
           "P625",
           "globecoordinate",
           GlobeCoordinateDataValue(40.67,
                                    -73.94,
                                    0.00027777777777778,
                                    "http://www.wikidata.org/entity/Q2")),
      "normal",
      Map(
        ("P580",
         List(
           Snak("value",
                "P580",
                "time",
                TimeDataValue("+00000001994-01-01T00:00:00Z",
                              0,
                              0,
                              0,
                              11,
                              "http://www.wikidata.org/entity/Q1985727")))))
    )

    val siteLink = SiteLink("dewiki", "New York City")
    val langItem = LangItem("en", "NYC")
    val desc = LangItem("en", "english desc")
    val labels = Map(("en", langItem))
    val descs = Map(("en", desc))
    val aliases = Map(("en", List(langItem)))
    val claims = List(Map(("P582", claim)))
    val siteLinks = Map(("de", siteLink))
    val item =
      WikidataItem("asde", "item", labels, descs, aliases, claims, siteLinks)
    val decoded = decode[WikidataItem](json)
    assert(decoded === Right(item))
  }
}
