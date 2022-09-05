package com.packtpub.beam.chapter2;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import com.packtpub.beam.util.MapToLines;
import com.packtpub.beam.util.Tokenize;

import akka.stream.impl.QueueSource.Input;

import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.checkerframework.checker.signedness.qual.SignedPositiveFromUnsigned;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.Window.OnTimeBehavior;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;

import java.util.Comparator;
import java.util.Arrays;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import lombok.Value;

import avro.shaded.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;

public class AverageWordLength {

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

        PCollection<String> words = lines
        .apply(Tokenize.of());
            
        calculateAverageWordLength(words, params.isDisableDefaults())
                .apply(WithKeys.of(""))
                .apply(
                    KafkaIO.<String, Double>write()
                    .withBootstrapServers(params.getBootstrapServer())
                    .withTopic(params.getOutputTopic())
                    .withKeySerializer(StringSerializer.class)
                    .withValueSerializer(DoubleSerializer.class)
        );
        pipeline.run().waitUntilFinish();
    }

    @VisibleForTesting
    static PCollection<Double> calculateAverageWordLength(
        PCollection<String> words) {
            return calculateAverageWordLength(words, false);
    }
    
    static PCollection<Double> calculateAverageWordLength(
        PCollection<String> words, boolean disableDefaults) {
                return words
                    .apply(
                        Window.<String>into(new GlobalWindows())
                        .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                        .accumulatingFiredPanes()
                        .withOnTimeBehavior(OnTimeBehavior.FIRE_IF_NON_EMPTY))
                    .apply(
                        disableDefaults
                        ? Combine.globally(new AverageFn()).withoutDefaults()
                        : Combine.globally(new AverageFn())
                    );

        
    }
    
    @VisibleForTesting
    static Params parseArgs(String[] args) {
        if (args.length < 3) {
            throw new IllegalArgumentException(
                "Expected at least 3 arguments: <bootstrapServer> <inputToopic> <outputTopic>"
            );
        }
        int pipelineArgsIndex = 3;
        boolean disableDefaults =
            args.length > pipelineArgsIndex && args[pipelineArgsIndex].equals("--withoutDefaults");
        
        return new Params(
            args[0],
            args[1],
            args[2],
            disableDefaults,
            Arrays.copyOfRange(args, pipelineArgsIndex, args.length)
        );
    }

    @Value
    @VisibleForTesting
    static class Params {
        String bootstrapServer;
        String inputTopic;
        String outputTopic;
        boolean disableDefaults;
        String[] remainingArgs;
    }

    @Value
    static class AverageAccumulator {
        long sumLength;
        long count;
    }

    static class AverageFn extends CombineFn<String, AverageAccumulator, Double> {
        @Override
        public AverageAccumulator createAccumulator() {
            return new AverageAccumulator(0, 0);
        }

        @Override
        public AverageAccumulator addInput(AverageAccumulator accumulator, String input) {
            return new AverageAccumulator(
                accumulator.getSumLength() + input.length(), accumulator.getCount() + 1);   
        }

        @Override
        public AverageAccumulator mergeAccumulators(Iterable<AverageAccumulator> accumulators) {
            long sumLength = 0L;
            long count = 0L;
            for (AverageAccumulator acc: accumulators) {
                sumLength += acc.getSumLength();
                count += acc.getCount();
            } 
            return new AverageAccumulator(sumLength, count);
        }

        @Override
        public Double extractOutput(AverageAccumulator accumulator) {
            return accumulator.getSumLength() / (double) accumulator.getCount();
        }

        @Override
        public Coder<AverageAccumulator> getAccumulatorCoder(
            CoderRegistry registry, Coder<String> inputCoder) {
                return new AverageAccumulatorCoder();
        }
    }

    static class AverageAccumulatorCoder extends CustomCoder<AverageAccumulator> {

        @Override
        public void encode(AverageAccumulator value, OutputStream outStream) throws CoderException, IOException {
            VarInt.encode(value.getSumLength(), outStream);
            VarInt.encode(value.getCount(), outStream);
        }

        @Override
        public AverageAccumulator decode(InputStream inStream) throws CoderException, IOException {
            return new AverageAccumulator(VarInt.decodeLong(inStream), VarInt.decodeLong(inStream));
        }
        
    }
}
