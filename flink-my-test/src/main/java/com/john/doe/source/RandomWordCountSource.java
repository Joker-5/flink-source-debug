package com.john.doe.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * Created by JOHN_DOE on 2023/7/15.
 */
public class RandomWordCountSource implements SourceFunction<String> {
    public static final int BOUND = 100;

    private Random rand = new Random();

    private volatile boolean running = true;

    private int counter = 100;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (running && counter < BOUND) {
            int first = rand.nextInt(BOUND / 2 - 1) + 1;
            int second = rand.nextInt(BOUND / 2 - 1) + 1;
            counter++;

            ctx.collect(genRandomWordLine(first, second));
            Thread.sleep(5000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    private String genRandomWordLine(int charMaxLimit, int wordMaxLimit) {
        StringBuilder sb = new StringBuilder();
        int leftLimit = 97;
        int rightLimit = 122;

        for (int i = 0; i < wordMaxLimit; i++) {
            int targetStringLen = rand.nextInt(charMaxLimit) + 1;
            StringBuilder buffer = new StringBuilder(targetStringLen);
            for (int j = 0; j < targetStringLen; j++) {
                int randomLimitedInt =
                        leftLimit + (int) (rand.nextFloat() * (rightLimit - leftLimit + 1));
                buffer.append((char) randomLimitedInt);
            }
            String generatedString = buffer.toString();
            if (sb.length() == 0) {
                sb.append(generatedString);
            } else {
                sb.append(" ").append(generatedString);
            }
        }

        return sb.toString();
    }

}
