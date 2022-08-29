package com.example

import java.util.Locale
import scala.collection.JavaConverters._
import org.apache.spark.sql.types.{StructField, _}
import com.example.protobuf.ComplexMessageProtos.ComplexMessage
import com.example.ProtoReflection.{Schema, schemaFor, schemaForClass, schemaForObject}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.{Column, Encoders, Row, SparkSession, functions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.functions.lower
import com.google.protobuf.util.JsonFormat
import com.google.protobuf.{Message, Timestamp}
import com.example.protobuf.ComplexMessageProtos.ComplexMessage
import com.example.protobuf.ComplexMessageProtos.SimpleMessage
import com.example.protobuf.OtherMessageProtos.OtherMessage
import org.apache.spark.sql.catalyst.util.InternalRowSet

import java.time.Instant


// import org.apache.avro.io.{BinaryDecoder, DecoderFactory}

object ProtoExample {

  def main(args: Array[String]): Unit = {
    //val sch = ProtoReflection.Schema(StructType(Seq(StructField("copies", IntegerType, true))), true)
    // new Column(ProtoDataToCatalyst[ComplexMessage](classOf[ComplexMessage], column.expr, topic,
    // Some(registryConfig)
    //
    // val registryConfig = Map("schema.registry.url" -> "http://localhost:8083")
    //
    //println(sf)
    //println(schp.dataType.prettyJson)


    val topic = "protobuf-topic"
    val kafkaUrl = "127.0.0.1:9092"
    val schemaRegistryUrl = "http://127.0.0.1:8081"
    val registryConfig = Map("schema.registry.url" -> "http://localhost:8081")

    // First time do a dummy SchemaForClass to invoke the exception
    // which seems to happen just once
    val otherMessage: OtherMessage = OtherMessage.newBuilder.setStringValue("other").setInt32Value(2).build()
    try {
      schemaForObject(otherMessage).dataType
    } catch {
      case _: Throwable =>
        println("==========Exception caught SchemaForClass========")
        //println("==========Trying again " + (schemaForObject(otherMessage).dataType))
        println("==========SchemaForClass " + schemaForClass(classOf[ComplexMessage]))
    }

    val spark = SparkSession.builder
      .appName("SparkProtoConsumer")
      .master("local[*]")
      .getOrCreate()

    spark.conf.set("spark.sql.debug.maxToStringFields", 100)


    //spark.streams.addListener(new QueryListener("SparkBenchmark.json"))

    import spark.implicits._

    // Create structured streaming DF to read from the topic.
    val rawTopicMessageDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaUrl)
      .option("subscribe", topic)
      .option("schema.registry.url", schemaRegistryUrl)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", 5000)
      .load()

    import scala.collection.JavaConverters._
    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[ComplexMessage]
    import spark.implicits._
    implicit val internalRowEncoder = Encoders.kryo[InternalRow]

    def from_proto(column: Column) : Column = {
      new Column(ProtoDataToCatalyst[ComplexMessage](classOf[ComplexMessage], column.expr, topic, Some(registryConfig)))
    }
    rawTopicMessageDF
      .select(from_proto(functions.col("value")).as("value"))
      .withColumn("simple_query", $"value.simple.query")
      .withColumn("simple_results_per_page", $"value.simple.results_per_page")
      .withColumn("simple_is_active", $"value.simple.is_active")
      .withColumn("simple_corpus", $"value.simple.corpus")
      .withColumn("simple_tstamp", $"value.simple.tstamp")
      .withColumn("complex_copies", $"value.copies")
      .withColumn("string_value", $"value.other.string_value")
      .withColumn("int32_value", $"value.other.int32_value")
      .withColumn("uint32_value", $"value.other.uint32_value")
      .withColumn("sint32_value", $"value.other.sint32_value")
      .withColumn("fixed32_value", $"value.other.fixed32_value")
      .withColumn("sfixed32_value", $"value.other.sfixed32_value")
      .withColumn("int64_value", $"value.other.int64_value")
      .withColumn("uint64_value", $"value.other.uint64_value")
      .withColumn("sint64_value", $"value.other.sint64_value")
      .withColumn("fixed64_value", $"value.other.fixed64_value")
      .withColumn("sfixed64_value", $"value.other.sfixed64_value")
      .withColumn("float_value", $"value.other.float_value")
      .withColumn("double_value", $"value.other.double_value")
      .withColumn("bool_value", $"value.other.bool_value")
      .withColumn("basic_enum", $"value.other.basic_enum")
      .withColumn("nested_enum", $"value.other.nested_enum")
      .withColumn("bytes_value", $"value.other.bytes_value")
      .withColumn("rint32_value", $"value.other.rint32_value")
      .withColumn("rstring_value", $"value.other.rstring_value")
      .withColumn("rbool_value", $"value.other.rbool_value")
      .withColumn("rint64_value", $"value.other.rint64_value")
      .withColumn("rfloat_value", $"value.other.rfloat_value")
      .withColumn("rdouble_value", $"value.other.rdouble_value")
      .withColumn("rbytes_value", $"value.other.rbytes_value")
      .withColumn("rnested_enum", $"value.other.rnested_enum")
      .withColumn("name_age", $"value.map_name_age")
      .withColumn("nested4_nested3", $"value.nested4.nested4_nested3")
      /*
      .withColumn("nested3", $"value.nested4.nested4_nested3"(0))
      .withColumn("nested2", $"nested3.nested3_nested2")
      .withColumn("nested1", $"nested2.nested2_nested1")
      .withColumn("nested1_string_value", $"nested1.nested1_string_value")
      .withColumn("nested1_int32_value", $"nested1.nested1_int32_value")*/
      .drop("value")
      .select($"simple_query", $"sfixed32_value", $"name_age", explode($"nested4_nested3"))
      .writeStream
      .outputMode("append")
      .option("numrows", 200)
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()
    /*
    val structureData = Seq(
      Row(Row("James ", "", "Smith"), "36636", "M", 3100),
      Row(Row("Michael ", "Rose", ""), "40288", "M", 4300),
      Row(Row("Robert ", "", "Williams"), "42114", "M", 1400),
      Row(Row("Maria ", "Anne", "Jones"), "39192", "F", 5500),
      Row(Row("Jen", "Mary", "Brown"), "", "F", -1)
    )

    val structureSchema = new StructType()
      .add("name", new StructType()
        .add("firstname", StringType)
        .add("middlename", StringType)
        .add("lastname", StringType))
      .add("id", StringType)
      .add("gender", StringType)
      .add("salary", IntegerType)


    val spark = SparkSession.builder
      .appName("ProtoExample")
      .master("local[*]")
      .getOrCreate()

    val df2 = spark.createDataFrame(
      spark.sparkContext.parallelize(structureData), structureSchema)
    df2.printSchema()
    df2.withColumn("lower_name", lower(functions.col("name.firstname")))
      .show()
    spark.stop() */

    /*
    val time = Instant.now
    val timestamp = Timestamp.newBuilder.setSeconds(time.getEpochSecond).setNanos(time.getNano).build

    val simpleMessage = SimpleMessage.newBuilder.setQuery("spark*protobuf*").setResultsPerPage(4).setIsActive(true).setCorpus(SimpleMessage.Corpus.WEB).setTstamp(timestamp)

    val otherMessage = OtherMessage.newBuilder.setName("other").setRecord(2)

    val compBuilder = ComplexMessage.newBuilder
    val compMessage = compBuilder.addSimple(simpleMessage).setCopies(1).setOther(otherMessage).build

    val jsonSchema = JsonFormat.printer().preservingProtoFieldNames().print(compMessage.asInstanceOf[Message])
    //println(jsonSchema.getClass.getName)

    val protoPath = Seq[String]()
    val catalystPath = Seq[String]()
    val sf = schemaFor(classOf[ComplexMessage])
    val catalystType = sf.dataType.asInstanceOf[StructType]
    val protoType = compMessage

    val protoSchemaHelper = new ProtoSchemaHelper(
      protoType, catalystType, protoPath, catalystPath)

    println(protoSchemaHelper.fieldMap)
    println(protoSchemaHelper.protoFieldArray)
    protoSchemaHelper.matchedFields.map {
      case ProtoMatchedField(catalystField, catalystPosition, protoField) =>
    }*/
  }
}