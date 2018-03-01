/*
 * Copyright (c) 2013-2018 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, and
 * you may not use this file except in compliance with the Apache License
 * Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Apache License Version 2.0 is distributed on an "AS
 * IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics
package snowplow
package enrich
package stream

// Scala
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.io.Source
import scala.util.Try
import collection.JavaConversions._

// Java
import java.util.Properties
import java.util.regex.Pattern

// Scala libraries
import pureconfig._
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

// Java libraries (test)
import com.hubspot.jinjava.Jinjava
import io.bfil.kafka.specs2.DefaultKafkaPorts
import io.bfil.kafka.specs2.EmbeddedKafkaContext

// Java libraries
import org.apache.commons.codec.binary.Base64

// Specs2
import org.specs2.matcher.{TraversableMatchers, TryMatchers}
import org.specs2.mutable.Specification

// This project
import good._
import model.EnrichConfig

class PiiEmitSpec extends Specification {

  val (testGoodIn, testGood, testBad, testPii) = ("testGoodIn", "testEnrichedGood", "testEnrichedBad", "testEnrichedUlgyPii")
  val kafkaHost = "127.0.0.1:9092"

  trait EmbeddedKafka extends EmbeddedKafkaContext with DefaultKafkaPorts with TryMatchers with TraversableMatchers {
    val kafkaTopics = Set(testGoodIn, testGood, testBad, testPii)
  }

  val jinJava = new Jinjava()
  val configValues = Map(
    "sourceType" -> "kafka",
    "sinkType" -> "kafka",
    "streamsInRaw" -> s"$testGoodIn",
    "outEnriched" -> s"$testGood",
    "outPii" -> s"$testPii",
    "outBad" -> s"$testBad",
    "partitionKeyName" -> "\"\"",
    "kafkaBrokers" -> s"$kafkaHost",
    "region" -> "\"\"",
    "enrichStreamsOutMaxBackoff" -> "\"\"",
    "enrichStreamsOutMinBackoff" -> "\"\"",
    "nsqdPort" -> "123",
    "nsqlookupdPort" -> "234",
    "bufferTimeThreshold" -> "1",
    "bufferRecordThreshold" -> "1",
    "bufferByteThreshold" -> "100000",
    "enrichAppName" -> "Jim",
    "enrichStreamsOutMaxBackoff" -> "1000",
    "enrichStreamsOutMinBackoff" -> "1000",
    "appName" -> "jim")

  val configRes = getClass.getResourceAsStream("/config.hocon.sample")
  val config = Source.fromInputStream(configRes).getLines.mkString("\n")
  val configInstance = jinJava.render(config, configValues)

  def decode(s: String) = Base64.decodeBase64(s)

  "Pii" should {
    "emit all events" in new EmbeddedKafka {
      // Input
      val inputGood = List(
                        decode(PagePingWithContextSpec.raw),
                        decode(PageViewWithContextSpec.raw),
                        decode(StructEventSpec.raw),
                        decode(StructEventWithContextSpec.raw),
                        decode(TransactionItemSpec.raw),
                        decode(TransactionSpec.raw))
      val (expectedGood, expectedBad, expectedPii) = (inputGood.size, 0, inputGood.size)

      import ExecutionContext.Implicits.global

      val app = Future {
        implicit def hint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

        val parsedConfig = ConfigFactory.parseString(configInstance).resolve()
        EnrichApp.run(
          loadConfigOrThrow[EnrichConfig](parsedConfig.getConfig("enrich")),
          SpecHelpers.resolver,
          SpecHelpers.enrichmentRegistry,
          None)
      }
      val producer = Future {
        val testKafkaPropertiesProducer = {
          val props = new Properties()
          props.put("bootstrap.servers", kafkaHost)
          props.put("client.id", "producer-george")
          props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
          props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
          props
        }
        val testProducer = new KafkaProducer[String, Array[Byte]](testKafkaPropertiesProducer)

        val events = inputGood
        events.foreach {
          r => testProducer.send(new ProducerRecord(testGoodIn, "key", r))
        }
        testProducer.flush
        testProducer.close
      }

      private def getRecords(cr: ConsumerRecords[String, String]): List[String] = cr.map(_.value).toList

      private def getConsumer(topics: List[String], expectedRecords: Int) = Future {
        val testKafkaPropertiesConsumer = {
          val props = new Properties()
          props.put("bootstrap.servers", kafkaHost)
          props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
          props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
          props.put("group.id", "consumer-jill")
          props
        }
        val testConsumerPii = new KafkaConsumer[String, String](testKafkaPropertiesConsumer)
        testConsumerPii.subscribe(topics)
        var records = getRecords(testConsumerPii.poll(100))
        while (records.size < expectedRecords) {
          records = records ++ getRecords(testConsumerPii.poll(100))
        }
        records
      }

      val producedBadRecords = getConsumer(List(testBad), expectedBad)
      val producedGoodRecords = getConsumer(List(testGood), expectedGood)
      val producedPiiRecords = getConsumer(List(testPii), expectedPii)

      val executionSec = 15L
      val allFutures = for {
        good <- producedGoodRecords
        bad <- producedBadRecords
        pii <- producedPiiRecords
      } yield (good, bad, pii)
      val t = Try {
        Await.result(allFutures, Duration(s"$executionSec sec"))
      }

      // Converts the entire "expected" results from the other tests to a regex string
      private def spaceJoinResult(expected: List[StringOrRegex]) = expected.flatMap({
        case JustRegex(r) => Some(r.toString)
        case JustString(s) if s.nonEmpty => Some(Pattern.quote(s))
        case _ => None
      }).mkString("\\s*")

      t must beSuccessfulTry.like {
        case (good: List[String], bad: List[String], pii: List[String]) => {
          (good must have size (expectedGood)) and
          (pii must have size (expectedPii)) and
          (bad must have size (expectedBad)) and
          (good must containMatch(spaceJoinResult(PagePingWithContextSpec.expected))) and
          (pii must contain(PagePingWithContextSpec.pii)) and
          (good must containMatch(spaceJoinResult(PageViewWithContextSpec.expected))) and
          (pii must contain(PageViewWithContextSpec.pii)) and
          (good must containMatch(spaceJoinResult(StructEventSpec.expected))) and
          (pii must contain(StructEventSpec.pii)) and
          (good must containMatch(spaceJoinResult(StructEventWithContextSpec.expected))) and
          (pii must contain(StructEventWithContextSpec.pii)) and
          (good must containMatch(spaceJoinResult(TransactionItemSpec.expected))) and
          (pii must contain(TransactionItemSpec.pii)) and
          (good must containMatch(spaceJoinResult(TransactionSpec.expected))) and
          (pii must contain(TransactionSpec.pii))
        }
      }
    }
  }
}
