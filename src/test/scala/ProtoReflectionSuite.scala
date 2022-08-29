
package com.example

import com.example.ProtoReflection._
import com.example.protobuf.ComplexMessageProtos.ComplexMessage
import com.example.protobuf.ComplexMessageProtos.SimpleMessage
import com.example.protobuf.OtherMessageProtos.OtherMessage
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class ProtoReflectionSuite extends AnyFunSuite {

  test("An empty Set should have size 0") {
    assert(Set.empty.size === 0)
  }

  test("Invoking head on an empty Set should produce NoSuchElementException") {
    assertThrows[NoSuchElementException] {
      Set.empty.head
    }
  }

  // First time, there is an exception and retry once.
  test(testName = "Other Message") {
    try {
      schemaForClass(classOf[OtherMessage]) shouldBe
        ProtoReflection.Schema(
          StructType(
            Seq(
              StructField("string_value", StringType, true),
              StructField("int32_value", IntegerType, true)
            )
          ),
          nullable = true
        )
    } catch {
      case _: Throwable =>
        schemaForClass(classOf[OtherMessage]) shouldBe
          ProtoReflection.Schema(
            StructType(
              Seq(
                StructField("name", StringType, true),
                StructField("record", IntegerType, true)
              )
            ),
            nullable = true
          )
    }
  }

  test(testName = "Simple Message") {
    schemaForClass(classOf[SimpleMessage]) shouldBe
      ProtoReflection.Schema(
        StructType(
          Seq(
              StructField("query", StringType, true),
              StructField("results_per_page", IntegerType, true),
              StructField("is_active", BooleanType, true),
              StructField("corpus", StringType, true),
              StructField("tstamp",
                StructType(
                  Seq(
                    StructField("seconds", LongType, true),
                    StructField("nanos", IntegerType, true)
                  )
                ), true)
            )
          ),
        nullable = true
      )
  }

  test(testName = "Complex Message") {
    schemaForClass(classOf[ComplexMessage]) shouldBe
      ProtoReflection.Schema(
        StructType(
          Seq(
            StructField("simple",
              ArrayType(
                StructType(
                  Seq(
                    StructField("query", StringType, true),
                    StructField("results_per_page", IntegerType, true),
                    StructField("is_active", BooleanType, true),
                    StructField("corpus", StringType, true),
                    StructField("tstamp",
                      StructType(
                        Seq(
                          StructField("seconds", LongType, true),
                          StructField("nanos", IntegerType, true)
                        )
                      ), true)
                  )
                ),
                containsNull = false
              ),
              false
            ),
            StructField("copies", IntegerType, true),
            StructField("other",
              StructType(
                Seq(
                  StructField("string_value", StringType, true),
                  StructField("int32_value", IntegerType, true)
                )
              ), true)
          )
        ),
        nullable = true
      )
    }

  test(testName = "Other Message using object") {
    val otherMessage: OtherMessage = OtherMessage.newBuilder.setStringValue("other").setInt32Value(2).build()
    schemaForObject(otherMessage) shouldBe
      ProtoReflection.Schema(
        StructType(
          Seq(
            StructField("string_value", StringType, true),
            StructField("int32_value", IntegerType, true)
          )
        ),
        nullable = true
      )
    }
  }