package com.packtpub.beam.chapter2;

import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.jupiter.api.Test;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;

public class MaxWordLengthTest {

    @Test
    public void testComputeMaxWordLength() {
        TestStream<String> lines = createInput();
        Pipeline pipeline = Pipeline.create();
        PCollection<String> input = pipeline.apply(lines);
        PCollection<String> output = MaxWordLength.computeLongestWord(input);
        PAssert.that(output)
            .containsInAnyOrder("a",
                "bb",
                "ccc",
                "ccc",
                "ccc");
        PAssert.that(output).inFinalPane(GlobalWindow.INSTANCE).containsInAnyOrder("ccc");

        pipeline.run();
    }

    private TestStream<String> createInput() {
        Instant now = Instant.now();

        return TestStream.create(StringUtf8Coder.of())
            .addElements(TimestampedValue.of("a", now))

            .addElements(TimestampedValue.of("bb", now.plus(10000)))
            .addElements(TimestampedValue.of("ccc", now.plus(20000)))
            .addElements(TimestampedValue.of("d", now.plus(30000))).advanceWatermarkToInfinity();
    }

//     @Test
//   public void testWordLength() {
//     Instant now = Instant.now();
//     TestStream<String> input =
//         TestStream.create(StringUtf8Coder.of())
//             .addElements(TimestampedValue.of("a", now))
//             .addElements(TimestampedValue.of("bb", now.plus(10000)))
//             .addElements(TimestampedValue.of("ccc", now.plus(20000)))
//             .addElements(TimestampedValue.of("d", now.plus(30000)))
//             .advanceWatermarkToInfinity();

//     Pipeline pipeline = Pipeline.create();
//     PCollection<String> strings = pipeline.apply(input);
//     PCollection<String> output = MaxWordLength.computeLongestWord(strings);
//     PAssert.that(output).containsInAnyOrder("a", "bb", "ccc", "ccc", "ccc");
//     PAssert.that(output).inFinalPane(GlobalWindow.INSTANCE).containsInAnyOrder("ccc");
//     pipeline.run();
//   }
    
}
