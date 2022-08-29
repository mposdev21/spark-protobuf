package com.example.schema

trait SchemaCoordinate

case class IdCoordinate(schemaId: Int) extends SchemaCoordinate

case class SubjectCoordinate(subject: String, id: Int) extends SchemaCoordinate
