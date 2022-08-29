package com.example.schema

import io.confluent.kafka.schemaregistry.SchemaProvider
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig

import scala.collection.JavaConverters._

class ConfluentRegistryClient(client: SchemaRegistryClient) extends AbstractConfluentRegistryClient(client) {
  def this(configs: Map[String,String]) = this(ConfluentRegistryClient.createClient(configs))
}

object ConfluentRegistryClient {

  private def createClient(configs: Map[String,String]) = {
    val settings = new KafkaProtobufDeserializerConfig(configs.asJava)
    val urls = settings.getSchemaRegistryUrls
    val maxSchemaObject = settings.getMaxSchemasPerSubject
    val providers: java.util.List[SchemaProvider] = new java.util.ArrayList[SchemaProvider]()
    providers.add(new ProtobufSchemaProvider())

    new CachedSchemaRegistryClient(urls, maxSchemaObject, providers, configs.asJava)
  }

}