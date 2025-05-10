package com.synthetic.config

import com.synthetic.models._

case class DataConfig(
                       fields: Map[String, FieldDistribution],
                       relationships: List[FieldRelationship],
                       generationParams: GenerationParams
                     )

case class FieldRelationship(
                              sourceField: String,
                              targetField: String,
                              strength: Double, // 0.0 to 1.0
                              relationshipType: RelationshipType
                            )

sealed trait RelationshipType
case object Linear extends RelationshipType
case object Quadratic extends RelationshipType
case object Logarithmic extends RelationshipType
