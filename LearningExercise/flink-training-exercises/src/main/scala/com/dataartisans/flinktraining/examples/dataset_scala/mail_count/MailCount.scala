/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flinktraining.examples.dataset_scala.mail_count

import com.dataartisans.flinktraining.dataset_preparation.MBoxParser
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

/**
 * Scala reference implementation for the "Mail Count" exercise of the Flink training.
 * The task of the exercise is to count the number of mails sent for each month and email address.
 *
 * Required parameters:
 * --input path-to-input-directory
 *
 */
object MailCount {
  def main(args: Array[String]) {

    // parse parameters
    val params = ParameterTool.fromArgs(args)
    val input = params.getRequired("input")

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // read the "time" and "sender" fields of the input data set as Strings
    val mails = env.readCsvFile[(String, String)](
      input,
      lineDelimiter = MBoxParser.MAIL_RECORD_DELIM,
      fieldDelimiter = MBoxParser.MAIL_FIELD_DELIM,
      includedFields = Array(1,2)
    )

    mails
      .map { m => (
                    // extract month from time string
                    m._1.substring(0, 7),
                    // extract email address from sender
                    m._2.substring(m._2.lastIndexOf("<") + 1, m._2.length - 1) ) }
      // group by month and sender and count the number of records per group

      //reduceGroup reduce list in group                    ("","",0) is initial value
      //                                                               c is result value  m is current
      .groupBy(0, 1).reduceGroup{xx =>  xx.foldLeft(("","",0))( (c, m) => (m._1, m._2, c._3+1)) }
      // print the result
      .print

  }
  /**
    * input
    * (2014-09-26-08:49:58,Fabian Hueske <fhueske@apache.org>)
    * (2014-09-12-14:50:38,Aljoscha Krettek <aljoscha@apache.org>)
    * (2014-09-30-09:16:29,Stephan Ewen <sewen@apache.org>)
    * output
    * (2014-09,fhueske@apache.org,16)
    * (2014-09,aljoscha@apache.org,13)
    * (2014-09,sewen@apache.org,24)
    * (2014-10,fhueske@apache.org,14)
    * (2014-10,aljoscha@apache.org,17)
  */
}
