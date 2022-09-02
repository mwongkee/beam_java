package com.packtpub.beam.chapter1;

import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import java.io.IOException;

public class FirstPipeline {
    public static void main(String[] args) throws IOException {
        // Read input file
        ClassLoader loader = Chapter1Demo.class.getClassLoader();
        String file = loader.getResource("lorem.txt").getFile();
        List<String> lines = Files.readAllLines(Paths.get(file), StandardCharsets.UTF_8);
        for (String line: lines) {
            System.out.println(line);
        }
    }
}