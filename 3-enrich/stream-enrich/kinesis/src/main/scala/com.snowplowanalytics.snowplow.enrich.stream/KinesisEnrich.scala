/*
 * Copyright (c) 2013-2017 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics
package snowplow
package enrich
package stream

import java.io.File
import java.net.URI

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.io.Source
import scala.sys.process._
import scala.util.control.NonFatal

import com.amazonaws.auth._
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ScanRequest}
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.GetObjectRequest
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import scalaz.{Sink => _, Source => _, _}
import Scalaz._

import common.enrichments.EnrichmentRegistry
import config._
import iglu.client.Resolver
import model.{AWSConfig, EnrichConfig}
import sinks.{KinesisSink, Sink}
import scalatracker.Tracker
import sources.KinesisSource

/** The main entry point for Stream Enrich for Kinesis. */
object KinesisEnrich extends App with Enrich {

  val DynamoDBRegex = "^dynamodb:([^/]*)/([^/]*)/([^/]*)$".r
  private val regexMsg = "'file:[filename]' or 'dynamodb:[region/table/key]'"

  private val trackerSourceProvider: Validation[String, (Option[Tracker], sources.Source, AWSCredentialsProvider)] = for {
    config <- parseConfig(args).validation
    (enrichConfig, resolverArg, enrichmentsArg, forceDownload) = config
    resolver <- parseResolver(resolverArg)
    enrichmentRegistry <- parseEnrichmentRegistry(enrichmentsArg)(resolver)
    _ <- cacheFiles(enrichmentRegistry, forceDownload).leftMap(_.head)
    tracker = enrichConfig.monitoring.map(c => SnowplowTracking.initializeTracker(c.snowplow))
    source <- getSource(enrichConfig, resolver, enrichmentRegistry, tracker)
    provider <- getProvider(enrichConfig.aws).validation
  } yield (tracker, source, provider)

  private val provider = trackerSourceProvider.map(_._3)

  trackerSourceProvider match {
    case Failure(e) =>
      System.err.println(e)
      System.exit(1)
    case Success((tracker, source, _)) =>
      tracker.foreach(SnowplowTracking.initializeSnowplowTracking(_))
      source.run()
  }

  override def getSource(
    enrichConfig: EnrichConfig,
    resolver: Resolver,
    enrichmentRegistry: EnrichmentRegistry,
    tracker: Option[Tracker]
  ): Validation[String, sources.Source] = {
    val kinesisConfig = enrichConfig.streams.kinesis
    val bufferConfig = enrichConfig.streams.buffer
    for {
      provider <- getProvider(enrichConfig.aws).validation
      goodSink = new ThreadLocal[sinks.Sink] {
        override def initialValue = new KinesisSink(
          provider, kinesisConfig, bufferConfig, enrichConfig.streams.out.enriched, tracker)
      }
      badSink = new ThreadLocal[sinks.Sink] {
        override def initialValue = new KinesisSink(
          provider, kinesisConfig, bufferConfig, enrichConfig.streams.out.bad, tracker)
      }
      source <- KinesisSource.createAndInitialize(
        enrichConfig, resolver, enrichmentRegistry, tracker, goodSink, badSink).validation
    } yield source
  }

  override val parser: scopt.OptionParser[FileConfig] =
    new scopt.OptionParser[FileConfig](generated.Settings.name) with FileConfigOptions {
      head(generated.Settings.name, generated.Settings.version)
      help("help")
      version("version")
      configOption()
      opt[String]("resolver").required().valueName("<resolver uri>")
        .text(s"Iglu resolver file, $regexMsg")
        .action((r: String, c: FileConfig) => c.copy(resolver = r))
        .validate(_ match {
          case FilepathRegex(_) | DynamoDBRegex(_) => success
          case _ => failure(s"Resolver doesn't match accepted uris: $regexMsg")
        })
      opt[String]("enrichments").optional().valueName("<enrichment directory uri>")
        .text(s"Directory of enrichment configuration JSONs, $regexMsg")
        .action((e: String, c: FileConfig) => c.copy(enrichmentsDir = Some(e)))
        .validate(_ match {
          case FilepathRegex(_) | DynamoDBRegex(_) => success
          case _ => failure(s"Enrichments directory doesn't match accepted uris: $regexMsg")
        })
      forceIpLookupsDownloadOption()
    }

  override def download(uri: URI, targetFile: File): Validation[String, Int] =
    uri.getScheme match {
      case "http" | "https" => (uri.toURL #> targetFile).!.success
      case "s3"             => downloadFromS3(uri, targetFile)
      case s                => s"Scheme $s for file $uri not supported".failure
    }

  /**
   * Downloads an object from S3 and returns whether or not it was successful.
   * @param uri The URI to reconstruct into a signed S3 URL
   * @param targetFile The file object to write to
   * @return the download result
   */
  private def downloadFromS3(uri: URI, targetFile: File): Validation[String, Int] =
    provider.map { p =>
      val s3Client = AmazonS3ClientBuilder
        .standard()
        .withCredentials(p)
        .build()
      val bucket = uri.getHost
      val key = uri.getPath match { // Need to remove leading '/'
        case s if s.charAt(0) == '/' => s.substring(1)
        case s => s
      }

      try {
        s3Client.getObject(new GetObjectRequest(bucket, key), targetFile)
        0
      } catch {
        case NonFatal(e) =>
          log.error(s"Error downloading $uri: ${e.getMessage}")
          1
      }
    }

  override def extractResolver(resolverArgument: String): Validation[String, String] =
    resolverArgument match {
      case FilepathRegex(filepath) =>
        val file = new File(filepath)
        if (file.exists) Source.fromFile(file).mkString.success
        else "Iglu resolver configuration file \"%s\" does not exist".format(filepath).failure
      case DynamoDBRegex(region, table, key) => lookupDynamoDBResolver(region, table, key)
      case _ => s"Resolver argument [$resolverArgument] must match $regexMsg".failure
    }

  /**
   * Fetch configuration from DynamoDB
   * Assumes the primary key is "id" and the configuration's key is "json"
   * @param region DynamoDB region, e.g. "eu-west-1"
   * @param table DynamoDB table containing the resolver
   * @param key The value of the primary key for the configuration
   * @return The JSON stored in DynamoDB
   */
  private def lookupDynamoDBResolver(
      region: String, table: String, key: String): Validation[String, String] = provider.map { p =>
    val dynamoDBClient = AmazonDynamoDBClientBuilder
      .standard()
      .withCredentials(p)
      .withEndpointConfiguration(new EndpointConfiguration(getDynamodbEndpoint(region), region))
      .build()
    val dynamoDB = new DynamoDB(dynamoDBClient)
    val item = dynamoDB.getTable(table).getItem("id", key)
    item.getString("json")
  }

  override def extractEnrichmentConfigs(
      enrichmentArg: Option[String]): Validation[String, String] = {
    val jsons: Validation[String, List[String]] = enrichmentArg.map {
      case FilepathRegex(dir) =>
        new File(dir).listFiles
          .filter(_.getName.endsWith(".json"))
          .map(scala.io.Source.fromFile(_).mkString)
          .toList
          .success
      case DynamoDBRegex(region, table, keyNamePrefix) =>
        lookupDynamoDBEnrichments(region, table, keyNamePrefix)
          .flatMap {
            case Nil => s"No enrichments found with prefix $keyNamePrefix".failure
            case js => js.success
          }
      case other => s"Enrichments argument [$other] must match $regexMsg".failure
    }.getOrElse(Nil.success)

    jsons.map { js =>
      val combinedJson =
        ("schema" -> "iglu:com.snowplowanalytics.snowplow/enrichments/jsonschema/1-0-0") ~
        ("data" -> js.toList.map(parse(_)))
      compact(combinedJson)
    }
  }

  /**
   * Get a list of enrichment JSONs from DynamoDB
   * @param region DynamoDB region, e.g. "eu-west-1"
   * @param table
   * @param keyNamePrefix Primary key prefix, e.g. "enrichments-"
   * @return List of JSONs
   */
  private def lookupDynamoDBEnrichments(region: String, table: String, keyNamePrefix: String
  ): Validation[String, List[String]] = provider.map { p =>
    val dynamoDBClient = AmazonDynamoDBClientBuilder
      .standard()
      .withCredentials(p)
      .withEndpointConfiguration(new EndpointConfiguration(getDynamodbEndpoint(region), region))
      .build()

    // Each scan can only return up to 1MB
    // See http://techtraits.com/cloud/nosql/2012/06/27/Amazon-DynamoDB--Understanding-Query-and-Scan-operations/
    @tailrec
    def partialScan(
      sofar: List[Map[String, String]] = Nil,
      lastEvaluatedKey: java.util.Map[String, AttributeValue] = null
    ): List[Map[String, String]] = {
      val scanRequest = new ScanRequest().withTableName(table)
      scanRequest.setExclusiveStartKey(lastEvaluatedKey)
      val lastResult = dynamoDBClient.scan(scanRequest)
      val combinedResults = sofar ++
        lastResult.getItems.asScala.map(_.asScala.toMap.mapValues(_.getS))
      lastResult.getLastEvaluatedKey match {
        case null => combinedResults
        case startKey => partialScan(combinedResults, startKey)
      }
    }
    val allItems = partialScan(Nil)
    allItems.filter { item => item.get("id") match {
        case Some(value) if value.startsWith(keyNamePrefix) => true
        case _ => false
      }
    }.flatMap(_.get("json"))
  }

  def getProvider(awsConfig: AWSConfig): \/[String, AWSCredentialsProvider] = {
    def isDefault(key: String): Boolean = key == "default"
    def isIam(key: String): Boolean = key == "iam"
    def isEnv(key: String): Boolean = key == "env"

    (awsConfig.accessKey, awsConfig.secretKey) match {
      case (a, s) if isDefault(a) && isDefault(s) =>
        new DefaultAWSCredentialsProviderChain().right
      case (a, s) if isDefault(a) || isDefault(s) =>
        "accessKey and secretKey must both be set to 'default' or neither".left
      case (a, s) if isIam(a) && isIam(s) =>
        InstanceProfileCredentialsProvider.getInstance().right
      case (a, s) if isIam(a) && isIam(s) =>
        "accessKey and secretKey must both be set to 'iam' or neither".left
      case (a, s) if isEnv(a) && isEnv(s) =>
        new EnvironmentVariableCredentialsProvider().right
      case (a, s) if isEnv(a) || isEnv(s) =>
        "accessKey and secretKey must both be set to 'env' or neither".left
      case (a, s) => new AWSStaticCredentialsProvider(new BasicAWSCredentials(a, s)).right
    }
  }

  private def getDynamodbEndpoint(region: String): String =
    region match {
      case cn@"cn-north-1" => s"https://dynamodb.$cn.amazonaws.com.cn"
      case _ => s"https://dynamodb.$region.amazonaws.com"
    }
}
