package com.example

import com.google.protobuf.Message
import com.google.protobuf.Descriptors.FieldDescriptor
import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.internal.SQLConf

case class ProtoMatchedField(
                              catalystField: StructField,
                              catalystPosition: Int,
                              protoField: FieldDescriptor)

class ProtoSchemaHelper (protoDescriptors: List[FieldDescriptor],
  catalystSchema: StructType,
  protoPath: Seq[String],
  catalystPath: Seq[String]) {

  //val protoFieldArray = protoSchema.getAllFields.asScala.toArray

  val fieldMap = protoDescriptors
    .groupBy(_.getName.toLowerCase(Locale.ROOT))
    .mapValues(_.toSeq) // toSeq needed for scala 2.13

  val matchedFields: Seq[ProtoMatchedField] = catalystSchema.zipWithIndex.flatMap {
    case (sqlField, sqlPos) =>
      getProtoField(sqlField.name, sqlPos).map(ProtoMatchedField(sqlField, sqlPos, _))
  }
  //println("==============catalystSchema======", catalystSchema)
  //println("==============matchedFields=======", matchedFields)

  def getProtoField(fieldName: String, catalystPos: Int): Option[FieldDescriptor] = {

    val candidates = fieldMap.getOrElse(fieldName.toLowerCase(Locale.ROOT), Seq.empty)
    //println("===========candidates============", fieldName, candidates)
    // search candidates, taking into account case sensitivity settings
    candidates.filter(f => SQLConf.get.resolver(f.getName, fieldName)) match {
      case Seq(protoField) =>
        //println("=========Some======", fieldName, protoField)
        Some(protoField)
      case Seq() => None
      case matches => throw new Exception(s"Searching for '$fieldName' in Proto " +
        s"gave ${matches.size} matches. Candidates: "
      )
    }
  }
}

