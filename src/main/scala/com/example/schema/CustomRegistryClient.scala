package com.example.schema

import io.confluent.kafka.schemaregistry.client.SchemaMetadata
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema

trait CustomRegistryClient {

  def getAllVersions(subject: String): java.util.List[Integer]

  def getLatestSchemaMetadata(subject: String): SchemaMetadata

  def getSchemaMetadata(subject: String, version: Int): SchemaMetadata

  def getBySubjectAndId(subject: String, schemaId: Int): ProtobufSchema

}
