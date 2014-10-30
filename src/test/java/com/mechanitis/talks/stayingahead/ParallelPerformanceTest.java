package com.mechanitis.talks.stayingahead;

import com.mongodb.MongoClient;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;

import java.net.UnknownHostException;
import java.util.List;
import java.util.stream.Collectors;

public class ParallelPerformanceTest {
    private static final double NUMBER_OF_ITERATIONS = 10_000;
    private Datastore datastore;

    @Before
    public void setUp() throws UnknownHostException {
        MongoClient mongoClient = new MongoClient();
        datastore = new Morphia().createDatastore(mongoClient, "Cafelito");
    }

    /* Check the Activity Monitor CPU Usage */
    @Test
    public void shouldPerformMapOKWhenSerial() {
        List<CoffeeShop> coffeeShops = datastore.find(CoffeeShop.class).asList();
        System.out.printf("Number Of Shops: %d%n", coffeeShops.size());
        System.out.println("Warming Up...");
        warmupMap(NUMBER_OF_ITERATIONS, coffeeShops);
        List<String> coffeeShopNames = null;

        long totalTime = 0;
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
            coffeeShopNames = coffeeShops.stream()
                                         .map(CoffeeShop::getName)
                                         .collect(Collectors.toList());
        }
        // then
        long endTime = System.currentTimeMillis();
        long timeTaken = endTime - startTime;
        System.out.printf("Time Taken: %d millis%n", timeTaken);
        System.out.printf("Number of coffee shop names: %d%n", coffeeShopNames.size());
        totalTime += timeTaken;
        System.out.printf("Mean time taken: " + totalTime / NUMBER_OF_ITERATIONS);
        //0.807 - 5 times data
        //0.1409 - basic data
    }

    @Test
    public void shouldPerformMapBetterWhenParallel() {
        // given
        List<CoffeeShop> coffeeShops = datastore.find(CoffeeShop.class).asList();
        System.out.printf("Number Of Shops: %d%n", coffeeShops.size());
        System.out.println("Warming Up...");
        warmupMapParallel(NUMBER_OF_ITERATIONS, coffeeShops);
        List<String> coffeeShopNames = null;

        long totalTime = 0;
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
            // MAGIC PARALLEL INCANTATION
            coffeeShopNames = coffeeShops.parallelStream()
                                         .map(CoffeeShop::getName)
                                         .collect(Collectors.toList());
        }
        long endTime = System.currentTimeMillis();
        long timeTaken = endTime - startTime;
        System.out.printf("Time Taken: %d millis%n", timeTaken);
        System.out.printf("Number of coffee shop names: %d%n", coffeeShopNames.size());
        totalTime += timeTaken;
        System.out.printf("Mean time taken: " + totalTime / NUMBER_OF_ITERATIONS);
        //0.5856 5x
        //0.1157 1x
    }

    @Test
    public void shouldPerformAnyMatchBetterWhenSerial() {
        // given
        List<CoffeeShop> coffeeShops = datastore.find(CoffeeShop.class).asList();
        System.out.printf("Number Of Shops: %d%n", coffeeShops.size());
        System.out.println("Warming Up...");
        warmupMatch(NUMBER_OF_ITERATIONS, coffeeShops);
        boolean starbucksFound = false;

        long totalTime = 0;
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
            starbucksFound = coffeeShops.stream()
                                        .anyMatch((coffeeShop) -> coffeeShop.getName().equals("Starbucks"));
        }
        long endTime = System.currentTimeMillis();
        long timeTaken = endTime - startTime;
        System.out.printf("Time Taken: %d millis%n", timeTaken);
        System.out.printf("Starbucks is in there? %s%n", starbucksFound);
        totalTime += timeTaken;
        System.out.printf("Mean time taken: " + totalTime / NUMBER_OF_ITERATIONS);
        //0.0013
    }

    @Test
    public void shouldPerformAnyMatchWorseWhenParallel() {
        // given
        List<CoffeeShop> coffeeShops = datastore.find(CoffeeShop.class).asList();
        System.out.printf("Number Of Shops: %d%n", coffeeShops.size());
        System.out.println("Warming Up...");
        warmupMatchParallel(NUMBER_OF_ITERATIONS, coffeeShops);
        boolean starbucksFound = false;

        long totalTime = 0;
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
            starbucksFound = coffeeShops.parallelStream()
                                        .anyMatch((coffeeShop) -> coffeeShop.getName().equals("Starbucks"));
        }
        long endTime = System.currentTimeMillis();
        long timeTaken = endTime - startTime;
        System.out.printf("Time Taken: %d millis%n", timeTaken);
        System.out.printf("Starbucks is in there? %s%n", starbucksFound);
        totalTime += timeTaken;
        System.out.printf("Mean time taken: " + totalTime / NUMBER_OF_ITERATIONS);
        //0.0145
    }

    @Test
    public void shouldPerformCountOKWhenSerial() {
        // given
        List<CoffeeShop> coffeeShops = datastore.find(CoffeeShop.class).asList();
        System.out.printf("Number Of Shops: %d%n", coffeeShops.size());
        System.out.println("Warming Up...");
        warmupCount(NUMBER_OF_ITERATIONS, coffeeShops);
        long numberOfShops = 0;

        long totalTime = 0;
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
            numberOfShops = coffeeShops.stream().count();
        }
        long endTime = System.currentTimeMillis();
        long timeTaken = endTime - startTime;
        System.out.printf("Time Taken: %d millis%n", timeTaken);
        System.out.printf("There were %d shops%n", numberOfShops);
        totalTime += timeTaken;
        System.out.printf("Mean time taken: " + totalTime / NUMBER_OF_ITERATIONS);
        //0.3625
    }

    @Test
    public void shouldPerformCountBetterWhenParallel() {
        // given
        List<CoffeeShop> coffeeShops = datastore.find(CoffeeShop.class).asList();
        System.out.printf("Number Of Shops: %d%n", coffeeShops.size());
        System.out.println("Warming Up...");
        warmupCountParallel(NUMBER_OF_ITERATIONS, coffeeShops);
        long numberOfShops = 0;

        long totalTime = 0;
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
            numberOfShops = coffeeShops.parallelStream().count();
        }
        long endTime = System.currentTimeMillis();
        long timeTaken = endTime - startTime;
        System.out.printf("Time Taken: %d millis%n", timeTaken);
        System.out.printf("There were %d shops%n", numberOfShops);
        totalTime += timeTaken;
        System.out.printf("Mean time taken: " + totalTime / NUMBER_OF_ITERATIONS);
        //0.0123
    }

    private void warmupMatch(final double numberOfIterations, final List<CoffeeShop> coffeeShops) {
        for (int i = 0; i < numberOfIterations; i++) {
            coffeeShops.stream().anyMatch((coffeeShop) -> coffeeShop.getName().equals("Starbucks"));
        }
        System.gc();
        System.gc();
    }

    private void warmupMatchParallel(final double numberOfIterations, final List<CoffeeShop> coffeeShops) {
        for (int i = 0; i < numberOfIterations; i++) {
            coffeeShops.parallelStream().anyMatch((coffeeShop) -> coffeeShop.getName().equals("Starbucks"));
        }
        System.gc();
        System.gc();
    }

    private void warmupCount(final double numberOfIterations, final List<CoffeeShop> coffeeShops) {
        for (int i = 0; i < numberOfIterations; i++) {
            coffeeShops.stream().count();
        }
        System.gc();
        System.gc();
    }

    private void warmupCountParallel(final double numberOfIterations, final List<CoffeeShop> coffeeShops) {
        for (int i = 0; i < numberOfIterations; i++) {
            coffeeShops.parallelStream().count();
        }
        System.gc();
        System.gc();
    }

    private void warmupMap(final double numberOfIterations, final List<CoffeeShop> coffeeShops) {
        for (int i = 0; i < numberOfIterations; i++) {
            coffeeShops.stream()
                       .map(CoffeeShop::getName)
                       .collect(Collectors.toList());
        }
        System.gc();
        System.gc();
    }

    private void warmupMapParallel(final double numberOfIterations, final List<CoffeeShop> coffeeShops) {
        for (int i = 0; i < numberOfIterations; i++) {
            coffeeShops.parallelStream()
                       .map(CoffeeShop::getName)
                       .collect(Collectors.toList());
        }
        System.gc();
        System.gc();
    }

}
