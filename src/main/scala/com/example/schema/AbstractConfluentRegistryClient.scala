package com.example.schema

import io.confluent.kafka.schemaregistry.client.{SchemaMetadata, SchemaRegistryClient}
import java.util

import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema

abstract class AbstractConfluentRegistryClient(client: SchemaRegistryClient) extends CustomRegistryClient {

  override def getAllVersions(subject: String): util.List[Integer] =
    client.getAllVersions(subject)

  override def getLatestSchemaMetadata(subject: String): SchemaMetadata =
    client.getLatestSchemaMetadata(subject)

  override def getSchemaMetadata(subject: String, version: Int): SchemaMetadata =
    client.getSchemaMetadata(subject, version)

  override def getBySubjectAndId(subject: String, schemaId: Int): ProtobufSchema = {
    client.getSchemaBySubjectAndId(subject, schemaId).asInstanceOf[ProtobufSchema]
  }
}
