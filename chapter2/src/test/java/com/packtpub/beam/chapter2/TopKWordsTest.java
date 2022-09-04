package com.packtpub.beam.chapter2;

import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.jupiter.api.Test;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;

public class TopKWordsTest {

    @Test
    public void testComputeStatsInWindows() {
        Duration windowDuration = Duration.standardSeconds(10);
        Instant now = Instant.now();
        Instant startOfTenSecondsWindow = now.plus(-now.getMillis() % windowDuration.getMillis());
        TestStream<String> lines = createInput(startOfTenSecondsWindow);
        Pipeline pipeline = Pipeline.create();
        PCollection<String> input = pipeline.apply(lines);
        PCollection<KV<String, Long>> output = TopKWords.countWordsInFixedWindows(input, 
        windowDuration, 3);
        PAssert.that(output)
            .containsInAnyOrder(
                KV.of("line", 3L),
                KV.of("first", 3L),
                KV.of("the", 4L),
                KV.of("line", 3L),
                KV.of("in", 2L),
                KV.of("window", 2L));
        pipeline.run();
    }

    private TestStream<String> createInput(Instant startOfTenSecondWindow) {
        return TestStream.create(StringUtf8Coder.of())
            .addElements(
                TimestampedValue.of("This is the first line.", startOfTenSecondWindow),
                TimestampedValue.of("This is the second line in the first window", startOfTenSecondWindow.plus(1000)),
                TimestampedValue.of("Last line in the first window", startOfTenSecondWindow.plus(2000)),
                TimestampedValue.of("This is another line, but in different window.", startOfTenSecondWindow.plus(10000)),
                TimestampedValue.of("Last line, in the same window as previous line", startOfTenSecondWindow.plus(11000))
            ).advanceWatermarkToInfinity();
    }
    
}
