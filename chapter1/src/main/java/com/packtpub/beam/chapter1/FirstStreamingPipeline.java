/**
 * Copyright 2021-2022 Packt Publishing Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.packtpub.beam.chapter1;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Timestamp;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import com.packtpub.beam.util.Tokenize;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.KV;
import com.packtpub.beam.util.PrintElements;

import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TimestampedValue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.joda.time.Instant;
import org.joda.time.Duration;

public class FirstStreamingPipeline {
  public static void main(String[] args) throws IOException {
    // Read input file
    ClassLoader loader = Chapter1Demo.class.getClassLoader();
    String file = loader.getResource("lorem.txt").getFile();
    List<String> lines = Files.readAllLines(Paths.get(file), StandardCharsets.UTF_8);
    for (String line : lines) {
      System.out.println(line);
    }

    // empty Pipeline.  container for both data (PCollection)
    // and operations (PTransforms)
    Pipeline pipeline = Pipeline.create();

    // TeamStream
    TestStream.Builder<String> streamBuilder = TestStream.create(StringUtf8Coder.of());

    // timestamp for each data element
    Instant now = Instant.now();

    // Add words with timestamp to stream
    List<TimestampedValue<String>> timestamped = 
      IntStream.range(0, lines.size())
        .mapToObj(i -> TimestampedValue.of(lines.get(i), now.plus(i)))
        .collect(Collectors.toList());

    for (TimestampedValue<String> value: timestamped) {
      streamBuilder = streamBuilder.addElements(value);
    }

    PCollection<String> input = pipeline.apply(streamBuilder.advanceWatermarkToInfinity());

    // lines to words
    PCollection<String> words = input.apply(Tokenize.of());
    // PCollection<String> windowed = words.apply(Window.<String>into(new GlobalWindows())
    Duration windowLength = Duration.standardSeconds(3);
    Duration allowedLateness = Duration.standardSeconds(1);
    PCollection<String> windowed = words.apply(Window.<String>into(FixedWindows.of(windowLength))

      .discardingFiredPanes()
      .triggering(AfterWatermark.pastEndOfWindow().withLateFirings(AfterPane.elementCountAtLeast(1)))
      .withAllowedLateness(allowedLateness)
    );

    windowed.apply(Count.perElement()).apply(PrintElements.of());

    pipeline.run().waitUntilFinish();
  }
}
