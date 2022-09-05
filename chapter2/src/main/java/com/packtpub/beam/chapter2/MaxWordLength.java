package com.packtpub.beam.chapter2;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import com.packtpub.beam.util.MapToLines;
import com.packtpub.beam.util.Tokenize;

import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.LongSerializer;

import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Top;

import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;

import org.apache.beam.sdk.transforms.windowing.Repeatedly;

import java.util.Comparator;
import java.util.Arrays;
import java.io.Serializable;
import lombok.Value;

import avro.shaded.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;

public class MaxWordLength {

    public static void main(String[] args) {
        Params params = parseArgs(args);
        PipelineOptions options = PipelineOptionsFactory.fromArgs(params.getRemainingArgs()).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> lines = 
            pipeline
            .apply(
                KafkaIO.<String, String>read()
                .withBootstrapServers(params.getBootstrapServer())
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withTopic(params.getInputTopic()))
            .apply(MapToLines.of());

        PCollection<String> output = 
            computeLongestWord(lines);

        output
            .apply(
                MapElements.into(
                TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                .via(e -> KV.of("", e)))
            .apply(
                KafkaIO.<String, String>write()
                .withBootstrapServers(params.getBootstrapServer())
                .withTopic(params.getOutputTopic())
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(StringSerializer.class)
        );
        pipeline.run().waitUntilFinish();
    }

    @VisibleForTesting
    static PCollection<String> computeLongestWord(
        PCollection<String> lines) {
            return lines
            .apply(Tokenize.of())
            .apply(
                Window.<String>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                    .withAllowedLateness(Duration.ZERO)
                    .accumulatingFiredPanes())
            .apply(
                Max.globally(
                    (Comparator<String> & Serializable)
                        (a, b) -> Long.compare(a.length(), b.length())));
    }
    
    @VisibleForTesting
    static Params parseArgs(String[] args) {
        if (args.length < 3) {
            throw new IllegalArgumentException(
                "Expected at least 3 arguments:  <bootstrapServer> <inputToopic> <outputTopic>"
            );
        }
        return new Params(
            Duration.standardSeconds(Integer.parseInt(args[0])),
            args[1],
            args[2],
            args[3],
            Integer.parseInt(args[4]),
            Arrays.copyOfRange(args, 5, args.length)
        );
    }

    @Value
    @VisibleForTesting
    static class Params {
        Duration windowLength;
        String bootstrapServer;
        String inputTopic;
        String outputTopic;
        int k;
        String[] remainingArgs;
    }
}
