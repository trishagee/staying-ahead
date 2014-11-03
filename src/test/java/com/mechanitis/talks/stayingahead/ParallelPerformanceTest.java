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
        //warmup
        List<CoffeeShop> coffeeShops = datastore.find(CoffeeShop.class).asList();
        System.out.printf("Number Of Shops: %d%n", coffeeShops.size());
        System.out.println("Warming Up...");
        warmupMap(NUMBER_OF_ITERATIONS, coffeeShops);
        List<String> coffeeShopNames = null;

        //execution
        System.out.println("Starting Test...");
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
            coffeeShopNames = coffeeShops.stream()
                                         .map(CoffeeShop::getName)
                                         .collect(Collectors.toList());
        }
        long endTime = System.currentTimeMillis();

        //results
        long timeTaken = endTime - startTime;
        System.out.printf("Time Taken: %d millis%n", timeTaken);
        System.out.printf("Number of coffee shop names: %d%n", coffeeShopNames.size());
        System.out.printf("Mean time taken: " + timeTaken / NUMBER_OF_ITERATIONS);
        //0.8070 5x
        //0.1409 1x
    }

    /* Check the Activity Monitor CPU Usage */
    @Test
    public void shouldPerformMapBetterWhenParallel() {
        //warmup
        List<CoffeeShop> coffeeShops = datastore.find(CoffeeShop.class).asList();
        System.out.printf("Number Of Shops: %d%n", coffeeShops.size());
        System.out.println("Warming Up...");
        warmupMapParallel(NUMBER_OF_ITERATIONS, coffeeShops);
        List<String> coffeeShopNames = null;

        //execution
        System.out.println("Starting Test...");
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
            // MAGIC PARALLEL INCANTATION
            coffeeShopNames = coffeeShops.parallelStream()
                                         .map(CoffeeShop::getName)
                                         .collect(Collectors.toList());
        }
        long endTime = System.currentTimeMillis();

        //results
        long timeTaken = endTime - startTime;
        System.out.printf("Time Taken: %d millis%n", timeTaken);
        System.out.printf("Number of coffee shop names: %d%n", coffeeShopNames.size());
        System.out.printf("Mean time taken: " + timeTaken / NUMBER_OF_ITERATIONS);
        //0.5856 5x
        //0.1157 1x
    }

    @Test
    public void shouldPerformAnyMatchBetterWhenSerial() {
        //warmup
        List<CoffeeShop> coffeeShops = datastore.find(CoffeeShop.class).asList();
        System.out.printf("Number Of Shops: %d%n", coffeeShops.size());
        System.out.println("Warming Up...");
        warmupMatch(NUMBER_OF_ITERATIONS, coffeeShops);
        boolean starbucksFound = false;

        //execution
        System.out.println("Starting Test...");
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
            starbucksFound = coffeeShops.stream()
                                        .anyMatch((coffeeShop) -> coffeeShop.getName().equals("Starbucks"));
        }
        long endTime = System.currentTimeMillis();

        //results
        long timeTaken = endTime - startTime;
        System.out.printf("Time Taken: %d millis%n", timeTaken);
        System.out.printf("Starbucks is in there? %s%n", starbucksFound);
        System.out.printf("Mean time taken: " + timeTaken / NUMBER_OF_ITERATIONS);
        //0.0013
    }

    @Test
    public void shouldPerformAnyMatchWorseWhenParallel() {
        //warmup
        List<CoffeeShop> coffeeShops = datastore.find(CoffeeShop.class).asList();
        System.out.printf("Number Of Shops: %d%n", coffeeShops.size());
        System.out.println("Warming Up...");
        warmupMatchParallel(NUMBER_OF_ITERATIONS, coffeeShops);
        boolean starbucksFound = false;

        //execution
        System.out.println("Starting Test...");
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
            starbucksFound = coffeeShops.parallelStream()
                                        .anyMatch((coffeeShop) -> coffeeShop.getName().equals("Starbucks"));
        }
        long endTime = System.currentTimeMillis();

        //results
        long timeTaken = endTime - startTime;
        System.out.printf("Time Taken: %d millis%n", timeTaken);
        System.out.printf("Starbucks is in there? %s%n", starbucksFound);
        System.out.printf("Mean time taken: " + timeTaken / NUMBER_OF_ITERATIONS);
        //0.0145
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
