package com.example

import com.google.protobuf.Descriptors.{EnumValueDescriptor, FieldDescriptor}
import com.google.protobuf.ByteString
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._
import com.google.protobuf.Message
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecificInternalRow, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

class ProtoDeserializer(
  rootProtoType: Message,
  rootCatalystType: DataType) {

  /**
   * Convert a sequence of hierarchical field names (like `Seq(foo, bar)`) into a human-readable
   * string representing the field, like "field 'foo.bar'". If `names` is empty, the string
   * "top-level record" is returned.
   */
  def toFieldStr(names: Seq[String]): String = names match {
    case Seq() => "top-level record"
    case n => s"field '${n.mkString(".")}'"
  }

  private val converter: Any => Option[Any] = try {
    rootCatalystType match {
      // A shortcut for empty schema.
      case st: StructType if st.isEmpty =>
        (_: Any) => Some(InternalRow.empty)

      case st: StructType =>
        val resultRow = new SpecificInternalRow(st.map(_.dataType))
        //println("==============StructType==========", st.map(_.dataType))
        val fieldUpdater = new RowUpdater(resultRow)

        val fieldDescriptors = rootProtoType.getAllFields.asScala.keys.toList
        val writer = getRecordWriter(fieldDescriptors, st, Nil, Nil)
        (data: Any) => {
          val record = data.asInstanceOf[Message]
          val skipRow = writer(fieldUpdater, record)
          if (skipRow) None else Some(resultRow)
        }

      case _ =>
        throw new Exception("Default case in Converter")
    }
  } catch {
    case ise: Exception => throw new Exception(
      s"Cannot convert Proto type $rootProtoType to SQL type ${rootCatalystType}.", ise)
  }

  def deserialize(data: Any): Option[Any] = converter(data)

  private def newArrayWriter(
                         protoField: FieldDescriptor,
                         catalystType: DataType,
                         protoPath: Seq[String],
                         catalystPath: Seq[String],
                         elementType: DataType,
                         containsNull: Boolean): (CatalystDataUpdater, Int, Any) => Unit = {


    val protoElementPath = protoPath :+ "element"
    val elementWriter = newWriter(protoField, elementType,
      protoElementPath, catalystPath :+ "element")
    (updater, ordinal, value) =>
      val collection = value.asInstanceOf[java.util.Collection[Any]]
      val result = createArrayData(elementType, collection.size())
      val elementUpdater = new ArrayDataUpdater(result)

      var i = 0
      val iter = collection.iterator()
      while (iter.hasNext) {
        val element = iter.next()
        if (element == null) {
          if (!containsNull) {
            throw new RuntimeException(
              s"Array value at path ${toFieldStr(protoElementPath)} is not allowed to be null")
          } else {
            elementUpdater.setNullAt(i)
          }
        } else {
          elementWriter(elementUpdater, i, element)
        }
        i += 1
      }

      updater.set(ordinal, result)
  }

  private def newWriter(
                         protoField: FieldDescriptor,
                         catalystType: DataType,
                         protoPath: Seq[String],
                         catalystPath: Seq[String]): (CatalystDataUpdater, Int, Any) => Unit = {

    (protoField.getJavaType, catalystType) match {
      case (BOOLEAN, BooleanType) => (updater, ordinal, value) =>
        updater.setBoolean(ordinal, value.asInstanceOf[Boolean])

      case (BOOLEAN, ArrayType(BooleanType, containsNull)) =>
        newArrayWriter(protoField, catalystType, protoPath,
          catalystPath, BooleanType, containsNull)

      case (INT, IntegerType) => (updater, ordinal, value) =>
        updater.setInt(ordinal, value.asInstanceOf[Int])

      case (INT, ArrayType(IntegerType, containsNull)) =>
         newArrayWriter(protoField, catalystType, protoPath,
           catalystPath, IntegerType, containsNull)

      case (LONG, LongType) => (updater, ordinal, value) =>
        updater.setLong(ordinal, value.asInstanceOf[Long])

      case (LONG, ArrayType(LongType, containsNull)) =>
        newArrayWriter(protoField, catalystType, protoPath,
          catalystPath, LongType, containsNull)

      case (FLOAT, FloatType) => (updater, ordinal, value) =>
        updater.setFloat(ordinal, value.asInstanceOf[Float])

      case (FLOAT, ArrayType(FloatType, containsNull)) =>
        newArrayWriter(protoField, catalystType, protoPath,
          catalystPath, FloatType, containsNull)

      case (DOUBLE, DoubleType) => (updater, ordinal, value) =>
        updater.setDouble(ordinal, value.asInstanceOf[Double])

      case (DOUBLE, ArrayType(DoubleType, containsNull)) =>
        newArrayWriter(protoField, catalystType, protoPath,
          catalystPath, DoubleType, containsNull)

      case (STRING, StringType) => (updater, ordinal, value) =>
        val str = value match {
          case s: String => UTF8String.fromString(s)
          case _ => throw new Exception("Invalid String format")
        }
        updater.set(ordinal, str)

      case (STRING, ArrayType(StringType, containsNull)) =>
        newArrayWriter(protoField, catalystType, protoPath,
          catalystPath, StringType, containsNull)

      case (BYTE_STRING, BinaryType) => (updater, ordinal, value) =>
        val byte_array = value match {
          case s: ByteString => s.toByteArray
          case _ => throw new Exception("Invalid ByteString format")
        }
        updater.set(ordinal, byte_array)

      case (BYTE_STRING, ArrayType(BinaryType, containsNull)) =>
        newArrayWriter(protoField, catalystType, protoPath,
          catalystPath, BinaryType, containsNull)

      case (ENUM, StringType) => (updater, ordinal, value) =>
        val str = value match {
          case s: EnumValueDescriptor => UTF8String.fromString(s.getName)
          case a: Any => throw new Exception(s"Invalid string format Enum ${a.getClass}, $value")
        }
        updater.set(ordinal, str)

      case (ENUM, ArrayType(StringType, containsNull)) =>
        newArrayWriter(protoField, catalystType, protoPath,
          catalystPath, StringType, containsNull)

      case (MESSAGE, st: StructType) =>
        val protoFieldDescriptors = protoField.getMessageType.getFields.asScala.toList
        val writeRecord =
          getRecordWriter(protoFieldDescriptors, st, protoPath, catalystPath)
        (updater, ordinal, value) =>
          val row = new SpecificInternalRow(st)
          writeRecord(new RowUpdater(row), value.asInstanceOf[Message])
          updater.set(ordinal, row)

      case (MESSAGE, ArrayType(st:StructType, containsNull)) =>
        newArrayWriter(protoField, catalystType, protoPath,
          catalystPath, st, containsNull)

      case _ => throw new Exception(s"=======Incompatible format ${protoField.getType}, $catalystType")
    }
  }

  private def getRecordWriter(
                               protoDescriptors: List[FieldDescriptor],
                               catalystType: StructType,
                               protoPath: Seq[String],
                               catalystPath: Seq[String]): (CatalystDataUpdater, Message) => Boolean = {

    val protoSchemaHelper = new ProtoSchemaHelper(
      protoDescriptors, catalystType, protoPath, catalystPath)

    // avroSchemaHelper.validateNoExtraCatalystFields(ignoreNullable = true)
    // no need to validateNoExtraAvroFields since extra Avro fields are ignored
    //println("=====protoPath, catalystPath====: ", protoPath, catalystPath)

    val (validFieldDescriptors, fieldWriters) = protoSchemaHelper.matchedFields.map {
      case ProtoMatchedField(catalystField, ordinal, protoField) =>
        val baseWriter = newWriter(protoField, catalystField.dataType,
          protoPath :+ protoField.getName, catalystPath :+ catalystField.name)
        val fieldWriter = (fieldUpdater: CatalystDataUpdater, value: Any) => {
          if (value == null) {
            fieldUpdater.setNullAt(ordinal)
          } else {
            baseWriter(fieldUpdater, ordinal, value)
          }
        }
        (protoField, fieldWriter)
    }.toArray.unzip

    (fieldUpdater, record) => {
      var i = 0
      var skipRow = false
      while (i < validFieldDescriptors.length && !skipRow) {
        fieldWriters(i)(fieldUpdater, record.getField(validFieldDescriptors(i)))
        //skipRow = applyFilters(i)
        i += 1
      }
      skipRow
    }
  }

/*
  private def getRecordWriter1(
                               protoType: Message,
                               catalystType: StructType,
                               protoPath: Seq[String],
                               catalystPath: Seq[String]): (CatalystDataUpdater, Message) => Boolean = {

    val protoSchemaHelper = new ProtoSchemaHelper(
      protoType, catalystType, protoPath, catalystPath)

    // avroSchemaHelper.validateNoExtraCatalystFields(ignoreNullable = true)
    // no need to validateNoExtraAvroFields since extra Avro fields are ignored

    val (validFieldDescriptors, fieldWriters) = protoSchemaHelper.matchedFields.map {
      case ProtoMatchedField(catalystField, ordinal, protoField) =>
        val baseWriter = newWriter(protoField, catalystField.dataType,
          protoPath :+ protoField.getName, catalystPath :+ catalystField.name)
        val fieldWriter = (fieldUpdater: CatalystDataUpdater, value: Any) => {
          if (value == null) {
            fieldUpdater.setNullAt(ordinal)
          } else {
            baseWriter(fieldUpdater, ordinal, value)
          }
        }
        (protoField, fieldWriter)
    }.toArray.unzip

    (fieldUpdater, record) => {
      var i = 0
      var skipRow = false
      while (i < validFieldDescriptors.length && !skipRow) {
        fieldWriters(i)(fieldUpdater, record.getField(validFieldDescriptors(i)))
        //skipRow = applyFilters(i)
        i += 1
      }
      skipRow
    }
  }*/

  private def createArrayData(elementType: DataType, length: Int): ArrayData = elementType match {
    case BooleanType => UnsafeArrayData.fromPrimitiveArray(new Array[Boolean](length))
    case ByteType => UnsafeArrayData.fromPrimitiveArray(new Array[Byte](length))
    case ShortType => UnsafeArrayData.fromPrimitiveArray(new Array[Short](length))
    case IntegerType => UnsafeArrayData.fromPrimitiveArray(new Array[Int](length))
    case LongType => UnsafeArrayData.fromPrimitiveArray(new Array[Long](length))
    case FloatType => UnsafeArrayData.fromPrimitiveArray(new Array[Float](length))
    case DoubleType => UnsafeArrayData.fromPrimitiveArray(new Array[Double](length))
    case _ => new GenericArrayData(new Array[Any](length))
  }

  /**
   * A base interface for updating values inside catalyst data structure like `InternalRow` and
   * `ArrayData`.
   */
  sealed trait CatalystDataUpdater {
    def set(ordinal: Int, value: Any): Unit

    def setNullAt(ordinal: Int): Unit = set(ordinal, null)
    def setBoolean(ordinal: Int, value: Boolean): Unit = set(ordinal, value)
    def setByte(ordinal: Int, value: Byte): Unit = set(ordinal, value)
    def setShort(ordinal: Int, value: Short): Unit = set(ordinal, value)
    def setInt(ordinal: Int, value: Int): Unit = set(ordinal, value)
    def setLong(ordinal: Int, value: Long): Unit = set(ordinal, value)
    def setDouble(ordinal: Int, value: Double): Unit = set(ordinal, value)
    def setFloat(ordinal: Int, value: Float): Unit = set(ordinal, value)
    def setDecimal(ordinal: Int, value: Decimal): Unit = set(ordinal, value)
  }

  final class RowUpdater(row: InternalRow) extends CatalystDataUpdater {
    override def set(ordinal: Int, value: Any): Unit = row.update(ordinal, value)
    override def setNullAt(ordinal: Int): Unit = row.setNullAt(ordinal)
    override def setBoolean(ordinal: Int, value: Boolean): Unit = row.setBoolean(ordinal, value)
    override def setByte(ordinal: Int, value: Byte): Unit = row.setByte(ordinal, value)
    override def setShort(ordinal: Int, value: Short): Unit = row.setShort(ordinal, value)
    override def setInt(ordinal: Int, value: Int): Unit = row.setInt(ordinal, value)
    override def setLong(ordinal: Int, value: Long): Unit = row.setLong(ordinal, value)
    override def setDouble(ordinal: Int, value: Double): Unit = row.setDouble(ordinal, value)
    override def setFloat(ordinal: Int, value: Float): Unit = row.setFloat(ordinal, value)
    override def setDecimal(ordinal: Int, value: Decimal): Unit =
      row.setDecimal(ordinal, value, value.precision)
  }

  final class ArrayDataUpdater(array: ArrayData) extends CatalystDataUpdater {
    override def set(ordinal: Int, value: Any): Unit = array.update(ordinal, value)
    override def setNullAt(ordinal: Int): Unit = array.setNullAt(ordinal)
    override def setBoolean(ordinal: Int, value: Boolean): Unit = array.setBoolean(ordinal, value)
    override def setByte(ordinal: Int, value: Byte): Unit = array.setByte(ordinal, value)
    override def setShort(ordinal: Int, value: Short): Unit = array.setShort(ordinal, value)
    override def setInt(ordinal: Int, value: Int): Unit = array.setInt(ordinal, value)
    override def setLong(ordinal: Int, value: Long): Unit = array.setLong(ordinal, value)
    override def setDouble(ordinal: Int, value: Double): Unit = array.setDouble(ordinal, value)
    override def setFloat(ordinal: Int, value: Float): Unit = array.setFloat(ordinal, value)
    override def setDecimal(ordinal: Int, value: Decimal): Unit = array.update(ordinal, value)
  }
}
