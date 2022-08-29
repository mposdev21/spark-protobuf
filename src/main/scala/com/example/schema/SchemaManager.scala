package com.example.schema

import io.confluent.kafka.schemaregistry.client.{SchemaRegistryClient}
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema
import org.apache.spark.internal.Logging


class SchemaManager(schemaRegistryClient: CustomRegistryClient) extends Logging {

  def this(schemaRegistryClient: SchemaRegistryClient) = this(new ConfluentRegistryClient(schemaRegistryClient))

  def getSchema(coordinate: SchemaCoordinate): ProtobufSchema = coordinate match {
    case SubjectCoordinate(subject, id) => getSchemaBySubjectAndId(subject, id)
  }

  def getSchemaBySubjectAndId(subject: String, id: Int): ProtobufSchema = {
    schemaRegistryClient.getBySubjectAndId(subject, id)
  }

}
