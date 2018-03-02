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

import scala.io.Source
import scala.util.Try
import scala.sys.process._

import com.typesafe.config.ConfigFactory
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.slf4j.LoggerFactory
import pureconfig._
import scalaz.{Sink => _, Source => _, _}
import Scalaz._

import common.enrichments.EnrichmentRegistry
import common.utils.JsonUtils
import config._
import iglu.client.Resolver
import model._
import scalatracker.Tracker

/** Interface for the entry point for Stream Enrich. */
trait Enrich {

  lazy val log = LoggerFactory.getLogger(getClass())

  val FilepathRegex = "^file:(.+)$".r
  private val regexMsg = "'file:[filename]'"

  def run(args: Array[String]): Unit = {
    val trackerSource = for {
      config <- parseConfig(args).validation
      (enrichConfig, resolverArg, enrichmentsArg, forceDownload) = config
      resolver <- parseResolver(resolverArg)
      enrichmentRegistry <- parseEnrichmentRegistry(enrichmentsArg)(resolver)
      _ <- cacheFiles(enrichmentRegistry, forceDownload)
      tracker = enrichConfig.monitoring.map(c => SnowplowTracking.initializeTracker(c.snowplow))
      source <- getSource(enrichConfig, resolver, enrichmentRegistry, tracker)
    } yield (tracker, source)

    trackerSource match {
      case Failure(e) =>
        System.err.println(e)
        System.exit(1)
      case Success((tracker, source)) =>
        tracker.foreach(SnowplowTracking.initializeSnowplowTracking)
        source.run()
    }
  }

  def getSource(
    enrichConfig: EnrichConfig,
    resolver: Resolver,
    enrichmentRegistry: EnrichmentRegistry,
    tracker: Option[Tracker]
  ): Validation[String, sources.Source]

  def parser: scopt.OptionParser[FileConfig]
  val localParser =
    new scopt.OptionParser[FileConfig](generated.Settings.name) with FileConfigOptions {
      head(generated.Settings.name, generated.Settings.version)
      help("help")
      version("version")
      configOption()
      localResolverOption()
      localEnrichmentsOption()
      forceIpLookupsDownloadOption()
    }

  def parseConfig(
      args: Array[String]): \/[String, (EnrichConfig, String, Option[String], Boolean)] = {
    implicit def hint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
    for {
      parsedCliArgs <- \/.fromEither(
        parser.parse(args, FileConfig()).toRight("Error while parsing command line arguments"))
      unparsedConfig = utils.fold(Try(ConfigFactory.parseFile(parsedCliArgs.config).resolve()))(
        t => t.getMessage.left,
        c => (c, parsedCliArgs.resolver, parsedCliArgs.enrichmentsDir, parsedCliArgs.forceDownload)
          .right
      )
      validatedConfig <- utils.filterOrElse(unparsedConfig)(
        t => t._1.hasPath("enrich"), "No top-level \"enrich\" could be found in the configuration")
      (config, resolverArg, enrichmentsArg, forceDownload) = validatedConfig
      parsedConfig <- utils.toEither(Try(loadConfigOrThrow[EnrichConfig](config.getConfig("enrich"))))
        .map(ec => (ec, resolverArg, enrichmentsArg, forceDownload))
        .leftMap(_.getMessage)
    } yield parsedConfig
  }

  def parseResolver(resolver: String): Validation[String, Resolver] = for {
    parsedResolver <- extractResolver(resolver)
    json <- JsonUtils.extractJson("", parsedResolver)
    resolver <- Resolver.parse(json).leftMap(_.toString)
  } yield resolver

  def parseEnrichmentRegistry(enrichmentsDir: Option[String])(
      implicit resolver: Resolver): Validation[String, EnrichmentRegistry] = for {
    enrichmentConfig <- extractEnrichmentConfigs(enrichmentsDir)
    registryConfig <- JsonUtils.extractJson("", enrichmentConfig)
    reg <- EnrichmentRegistry.parse(fromJsonNode(registryConfig), false).leftMap(_.toString)
  } yield reg

  def download(uri: URI, targetFile: File): Validation[String, Int]
  val httpDownloader = (uri: URI, targetFile: File) =>
    uri.getScheme match {
      case "http" | "https" => (uri.toURL #> targetFile).!.success
      case s                => s"Scheme $s for file $uri not supported".failure
    }

  /**
   * Download the IP lookup files locally.
   * @param registry Enrichment registry
   * @param forceDownload CLI flag that invalidates the cached files on each startup
   * @param provider AWS credentials provider necessary to download files from S3
   * @return a list of failures
   */
  def cacheFiles(
    registry: EnrichmentRegistry,
    forceDownload: Boolean
  ): ValidationNel[String, List[Int]] =
    registry.getIpLookupsEnrichment
      .map(_.dbsToCache)
      .toList.flatten
      .map { case (uri, path) =>
        (new java.net.URI(uri.toString.replaceAll("(?<!(http:|https:|s3:))//", "/")),
          new File(path))
      }
      .filter { case (_, targetFile) => forceDownload || targetFile.length == 0L }
      .map { case (cleanURI, targetFile) =>
        download(cleanURI, targetFile)
          .flatMap {
            case i if i != 0 => s"Attempt to download $cleanURI to $targetFile failed".failure
            case o => o.success
          }
          .toValidationNel
      }
      .sequenceU

  /**
   * Return a JSON string based on the resolver argument
   * @param resolverArgument location of the resolver
   * @return JSON from a local file or stored in DynamoDB
   */
  def extractResolver(resolverArgument: String): Validation[String, String]
  val localResolverExtractor = (resolverArgument: String) => resolverArgument match {
    case FilepathRegex(filepath) =>
      val file = new File(filepath)
      if (file.exists) {
        Source.fromFile(file).mkString.success
      } else {
        "Iglu resolver configuration file \"%s\" does not exist".format(filepath).failure
      }
    case _ => s"Resolver argument [$resolverArgument] must match $regexMsg".failure
  }

  /**
   * Return an enrichment configuration JSON based on the enrichments argument
   * @param enrichmentArgument location of the enrichments directory
   * @return JSON containing configuration for all enrichments
   */
  def extractEnrichmentConfigs(enrichmentArgument: Option[String]): Validation[String, String]
  val localEnrichmentConfigsExtractor = (enrichmentArgument: Option[String]) => {
    val jsons: Validation[String, List[String]] = enrichmentArgument.map {
      case FilepathRegex(path) =>
        new File(path).listFiles
          .filter(_.getName.endsWith(".json"))
          .map(scala.io.Source.fromFile(_).mkString)
          .toList
          .success
      case other => s"Enrichments argument [$other] must match $regexMsg".failure
    }.getOrElse(Nil.success)

    jsons.map { js =>
      val combinedJson =
        ("schema" -> "iglu:com.snowplowanalytics.snowplow/enrichments/jsonschema/1-0-0") ~
        ("data" -> js.toList.map(parse(_)))
      compact(combinedJson)
    }
  }
}
