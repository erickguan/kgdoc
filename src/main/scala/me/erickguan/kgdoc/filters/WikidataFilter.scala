package me.erickguan.kgdoc.filters

import me.erickguan.kgdoc.json._

object WikidataFilter {
  def byProperty(property: String, item: WikidataItem): Boolean =
    item.claims.forall(_._1 == property)

  val SubclassPropertyId = "P279"

  val entityFromDataValue: PartialFunction[DataValue, String] = {
    case WikibaseEntityIdDataValue(_, numericId) => s"Q$numericId"
  }

  val entityFromSnak: PartialFunction[Snak, String] = {
    case Snak(_, _, datatype, datavalue) if datatype == "wikibase-item" =>
      entityFromDataValue(datavalue)
  }

  val entityFromClaim: PartialFunction[Claim, String] = {
    case Claim(_, mainsnak, rank, _)
        if rank == "normal" || rank == "preferred" =>
      entityFromSnak(mainsnak)
  }

  def isEntity(item: WikidataItem): Boolean = item.itemType == "item"

  /**
    * Determines if an entity has certain predicate and object
    * @param propertyId property id in a claim
    * @param objectEntityId object id in a claim
    * @param item processed item
    * @return a boolean
    */
  def byProperty(propertyId: String,
                 objectEntityId: String,
                 item: WikidataItem): Boolean =
    item.claims.get(propertyId) match {
      case Some(claims) => claims.exists(entityFromClaim(_) == objectEntityId)
      case None         => false
    }

  /**
    * Determines if an entity is certain subclass of given parameter
    * @param subclassId subclass id in a claim
    * @param item processed item
    * @return a boolean
    */
  def bySubclass(subclassId: String, item: WikidataItem): Boolean =
    byProperty(SubclassPropertyId, subclassId, item)

  val MetaBibliographicEntityIds = List("Q732577", "Q191067")
  val InstanceOfPropertyId = "P31"

  /**
    * Determines if an entity is certain instance of given parameter
    * @param instanceId instance id in a claim
    * @param item processed item
    * @return a boolean
    */
  def byInstance(instanceId: String, item: WikidataItem): Boolean =
    byProperty(InstanceOfPropertyId, instanceId, item)

  /**
    * Determines if an class entity is a subclass of bibliographic items
    * @param item processed item
    * @return a boolean
    */
  def byBibliographicClass(item: WikidataItem): Boolean =
    MetaBibliographicEntityIds.exists(bySubclass(_, item))

  /**
    * Determines if an entity is a instance of these entities
    * @param entityIds entity list
    * @param item processed item
    * @return a boolean
    */
  def byInstanceOfEntities(entityIds: Iterable[String],
                           item: WikidataItem): Boolean =
    entityIds.exists(byInstance(_, item))

}
