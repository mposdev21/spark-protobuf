package com.example.schema
import scala.collection.concurrent
import scala.util.Try
import scala.util.control.NonFatal
import org.apache.spark.internal.Logging

object SchemaManagerFactory extends Logging {

  private val clientInstances: concurrent.Map[Map[String, String], CustomRegistryClient] = concurrent.TrieMap()

  def addSRClientInstance(configs: Map[String, String], client: CustomRegistryClient): Unit = {
    clientInstances.put(configs, client)
  }

  def resetSRClientInstance(): Unit = {
    clientInstances.clear()
  }
  def create(configs: Map[String,String]): SchemaManager = new SchemaManager(getOrCreateRegistryClient(configs))

  private def getOrCreateRegistryClient(configs: Map[String,String]): CustomRegistryClient = {
    clientInstances.getOrElseUpdate(configs, {
      if (configs.contains("")) {
        try {
          val clazz = Class.forName("custom.registryClient.class")
          logInfo(msg = s"Configuring new Schema Registry client of type '${clazz.getCanonicalName}'")
          Try(clazz.getConstructor(classOf[Map[String, String]]).newInstance(configs))
            .recover { case _: NoSuchMethodException =>
              clazz.getConstructor().newInstance()
            }
            .get
            .asInstanceOf[CustomRegistryClient]
        } catch {
          case e if NonFatal(e) =>
            throw new IllegalArgumentException("Custom registry client must implement CustomRegistryClient " +
              "and have parameterless or Map[String, String] accepting constructor", e)
        }
      } else {
        logInfo(msg = s"Configuring new Schema Registry client of type ConfluentRegistryClient")
        new ConfluentRegistryClient(configs)
      }
    })
  }
}
