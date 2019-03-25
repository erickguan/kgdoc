package me.erickguan.kgdoc.filters

import me.erickguan.kgdoc.json._

object WikidataItemFilter {
  val SubclassPropertyId = "P279"
  val EntityType = "item"

  val MetaBibliographicEntityIds = List("Q732577", "Q191067")
  val InstanceOfPropertyId = "P31"

  val entityFromDataValue: PartialFunction[DataValue, String] = {
    case WikibaseEntityIdDataValue(entityType, id)
        if entityType == EntityType =>
      id
  }

  val entityFromOptionDataValue: PartialFunction[Option[DataValue], String] = {
    case Some(dv) => entityFromDataValue(dv)
  }

  val entityFromSnak: PartialFunction[Snak, String] = {
    case Snak(snaktype, _, datatype, datavalue)
        if snaktype == "value" && datatype == "wikibase-item" && entityFromOptionDataValue
          .isDefinedAt(datavalue) =>
      entityFromOptionDataValue(datavalue)
  }

  val entityFromClaim: PartialFunction[Claim, String] = {
    case Claim(_, mainsnak, rank, _)
        if (rank == "normal" || rank == "preferred") && entityFromSnak
          .isDefinedAt(mainsnak) =>
      entityFromSnak(mainsnak)
  }
}

class WikidataItemFilter(item: WikidataItem) {
  /**
    * Determines if the item has a property
    * @param propertyId property id in a claim
    * @return a boolean
    */
  def byProperty(propertyId: String): Boolean =
    item.claims.forall(_._1 == propertyId)

  def isEntity: Boolean = item.itemType == WikidataItemFilter.EntityType

  /**
    * Determines if the entity has certain predicate and object
    * @param propertyId property id in a claim
    * @param objectEntityId object id in a claim
    * @return a boolean
    */
  def byProperty(propertyId: String, objectEntityId: String): Boolean =
    item.claims.get(propertyId) match {
      case Some(claims) =>
        claims.exists(
          c =>
            WikidataItemFilter.entityFromClaim
              .isDefinedAt(c) && WikidataItemFilter
              .entityFromClaim(c) == objectEntityId)
      case None => false
    }

  /**
    * Determines if the entity is certain subclass of given parameter
    * @param subclassId subclass id in a claim
    * @return a boolean
    */
  def bySubclass(subclassId: String): Boolean =
    byProperty(WikidataItemFilter.SubclassPropertyId, subclassId)

  /**
    * Determines if the entity is certain instance of given parameter
    * @param instanceId instance id in a claim
    * @return a boolean
    */
  def byInstance(instanceId: String): Boolean =
    byProperty(WikidataItemFilter.InstanceOfPropertyId, instanceId)

  /**
    * Determines if an class entity is a subclass of bibliographic items
    * @return a boolean
    */
  def byBibliographicClass(): Boolean =
    WikidataItemFilter.MetaBibliographicEntityIds.exists(bySubclass)

  /**
    * Determines if the entity is a instance of these entities
    * @param entityIds entity list
    * @return a boolean
    */
  def byInstanceOfEntities(entityIds: Iterable[String]): Boolean =
    entityIds.exists(byInstance)

}
