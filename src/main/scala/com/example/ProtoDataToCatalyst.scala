package com.example

import com.example.ProtoReflection.{schemaForClass, schemaForMessage, schemaForObject}
import com.example.schema.SchemaManagerFactory
import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.{DynamicMessage, Message}
import io.confluent.kafka.schemaregistry.protobuf.{MessageIndexes, ProtobufSchema}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.types._

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import scala.collection.mutable
import scala.util.Try

case class ProtoDataToCatalyst[T <: com.google.protobuf.AbstractMessage](
                                                                          val clazz: Class[T],
                                                                          child: Expression,
                                                                          topicName: String,
                                                                          schemaRegistryConf: Option[Map[String, String]])
  extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[BinaryType.type] = Seq(BinaryType)

  @transient private lazy val subject =  topicName + "-value"

  import scala.collection.JavaConverters._

  override def dataType: DataType = {
    try {
      schemaForClass(clazz).dataType
    } catch {
      case _: Throwable =>
        println("Exception caught SchemaForClass")
        schemaForClass(clazz).dataType
    }
  }

  @transient private lazy val MAGIC_BYTE = 0x0

  @transient private lazy val SCHEMA_ID_SIZE_BYTES = 4

  @transient private lazy val schemaManager = SchemaManagerFactory.create(schemaRegistryConf.get)

  @transient private lazy val schemaCache = new mutable.HashMap[Tuple2[String, Int], ProtobufSchema]()

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    //val expr = ctx.addReferenceObj("this", this)
    //defineCodeGen(ctx, ev, input =>
      //s"(${boxedType(ctx, dataType)})$expr.nullSafeEval($input)")

    val expr = ctx.addReferenceObj("this", this)
    nullSafeCodeGen(ctx, ev, eval => {
      val result = ctx.freshName("result")
      val dt = CodeGenerator.boxedType(dataType)
      s"""
            $dt $result = ($dt) $expr.nullSafeEval($eval);
            if ($result == null) {
              ${ev.isNull} = true;
            } else {
              ${ev.value} = $result;
            }
          """
    })
  }

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

  override def nullSafeEval(input: Any): Any = {
    val binary = input.asInstanceOf[Array[Byte]]
    val decoded = protoBytesToMessage(binary)

    // decoded was read using the latest schema. Use that to derive
    // the catalyst type
    val dType = schemaForMessage(decoded.asInstanceOf[Message]).dataType
    val deserializer = new ProtoDeserializer(decoded.asInstanceOf[Message], dType)
    val deserialized = deserializer.deserialize(decoded)
    deserialized.get
    /*
    val jsonString = JsonFormat.printer().preservingProtoFieldNames().print(decoded.asInstanceOf[Message])
    InternalRow(UTF8String.fromString(jsonString))*/
  }

  def protoBytesToMessage[T <: com.google.protobuf.AbstractMessage](
                                                                     payload: Array[Byte]) : Object = {
    val buffer = ByteBuffer.wrap(payload)
    if (buffer.get() !=  MAGIC_BYTE) {
      throw new Exception("Unknown magic byte!")
    }

    val schemaId = buffer.getInt()
    val indexes = MessageIndexes.readFrom(buffer)
    val length = buffer.limit() - 1 - SCHEMA_ID_SIZE_BYTES
    val start = buffer.position() + buffer.arrayOffset()

    val schema = schemaCache.getOrElseUpdate(new Pair[String, Int](subject, schemaId), {
      println(f"getting cached Schema for subject, schemaId : ${subject}, ${schemaId}")
      schemaManager.getSchemaBySubjectAndId(subject, schemaId)
    })

    val parseMethod = clazz.getDeclaredMethod("parseFrom", classOf[ByteBuffer])
    if (parseMethod != null) {
      try {
        val value = parseMethod.invoke(null, buffer)
        return value
      } catch {
        case e: Exception =>
          throw new Exception("Not a valid protobuf builder", e);
      }
    } else {
      
      val descriptor: Descriptor = schema.toDescriptor()
      if (descriptor == null) {
        throw new Exception("Could not find descriptor with name " + schema.name())
      }
      DynamicMessage.parseFrom(descriptor, new ByteArrayInputStream(buffer.array(), start, length))
    }
  }

  private def boxedType(ctx: CodegenContext, dataType: DataType): String = {
    val tryBoxedTypeSpark2_4 = Try {
      CodeGenerator
        .getClass
        .getMethod("boxedType", classOf[DataType])
        .invoke(CodeGenerator, dataType)
    }

    val boxedType = tryBoxedTypeSpark2_4.getOrElse {
      classOf[CodegenContext]
        .getMethod("boxedType", classOf[DataType])
        .invoke(ctx, dataType)
    }

    boxedType.asInstanceOf[String]
  }
}