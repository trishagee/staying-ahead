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
    private Datastore datastore;

    @Before
    public void setUp() throws UnknownHostException {
        MongoClient mongoClient = new MongoClient();
        datastore = new Morphia().createDatastore(mongoClient, "Cafelito");
    }

    @Test
    public void shouldPerformMapOKWhenSerial() {
        List<CoffeeShop> coffeeShops = datastore.find(CoffeeShop.class).asList();
        System.out.printf("Number Of Shops: %d%n", coffeeShops.size());
        System.out.println("Warming Up...");
        warmupMap(100, coffeeShops);

        long totalTime = 0;
        for (int i = 0; i < 50; i++) {
            // given
            System.out.printf("--- Run: %d ---%n", i);

            // when
            long startTime = System.currentTimeMillis();
            List<String> coffeeShopNames = coffeeShops.stream()
                                                      .map(CoffeeShop::getName)
                                                      .collect(Collectors.toList());
            long endTime = System.currentTimeMillis();

            // then
            long timeTaken = endTime - startTime;
            System.out.printf("Time Taken: %d millis%n", timeTaken);
            System.out.printf("Number of coffee shop names: %d%n", coffeeShopNames.size());
            totalTime += timeTaken;
        }
        System.out.printf("Mean time taken: " + totalTime / 100);
    }

    @Test
    public void shouldPerformMapBetterWhenParallel() {
        // given
        List<CoffeeShop> coffeeShops = datastore.find(CoffeeShop.class).asList();
        System.out.printf("Number Of Shops: %d%n", coffeeShops.size());
        System.out.println("Warming Up...");
        warmupMapParallel(100, coffeeShops);

        long totalTime = 0;
        for (int i = 0; i < 50; i++) {
            System.out.printf("--- Run: %d ---%n", i);

            // when
            long startTime = System.currentTimeMillis();
            List<String> coffeeShopNames = coffeeShops.parallelStream()
                                                           .map(CoffeeShop::getName)
                                                           .collect(Collectors.toList());
            long endTime = System.currentTimeMillis();

            // then
            long timeTaken = endTime - startTime;
            System.out.printf("Time Taken: %d millis%n", timeTaken);
            System.out.printf("Number of coffee shop names: %d%n", coffeeShopNames.size());
            totalTime += timeTaken;
        }
        System.out.printf("Mean time taken: " + totalTime / 100);
    }

    @Test
    public void shouldPerformMatchOKWhenSerial() {
        // given
        List<CoffeeShop> coffeeShops = datastore.find(CoffeeShop.class).asList();
        System.out.printf("Number Of Shops: %d%n", coffeeShops.size());
        System.out.println("Warming Up...");
        warmupMatch(100, coffeeShops);

        long totalTime = 0;
        for (int i = 0; i < 50; i++) {
            System.out.printf("--- Run: %d ---%n", i);

            // when
            long startTime = System.currentTimeMillis();
            boolean starbucksFound = coffeeShops.stream()
                                                .anyMatch((coffeeShop) -> coffeeShop.getName().equals("Starbucks"));
            long endTime = System.currentTimeMillis();

            // then
            long timeTaken = endTime - startTime;
            System.out.printf("Time Taken: %d millis%n", timeTaken);
            System.out.printf("Starbucks is in there? %s%n", starbucksFound);
            totalTime += timeTaken;
        }
        System.out.printf("Mean time taken: " + totalTime / 100);
    }

    @Test
    public void shouldPerformMatchBetterWhenParallel() {
        // given
        List<CoffeeShop> coffeeShops = datastore.find(CoffeeShop.class).asList();
        System.out.printf("Number Of Shops: %d%n", coffeeShops.size());
        System.out.println("Warming Up...");
        warmupMatchParallel(100, coffeeShops);

        long totalTime = 0;
        for (int i = 0; i < 50; i++) {
            System.out.printf("--- Run: %d ---%n", i);

            // when
            long startTime = System.currentTimeMillis();
            boolean starbucksFound = coffeeShops.parallelStream()
                                                .anyMatch((coffeeShop) -> coffeeShop.getName().equals("Starbucks"));
            long endTime = System.currentTimeMillis();

            // then
            long timeTaken = endTime - startTime;
            System.out.printf("Time Taken: %d millis%n", timeTaken);
            System.out.printf("Starbucks is in there? %s%n", starbucksFound);
            totalTime += timeTaken;
        }
        System.out.printf("Mean time taken: " + totalTime / 100);
    }

    @Test
    public void shouldPerformCountOKWhenSerial() {
        // given
        List<CoffeeShop> coffeeShops = datastore.find(CoffeeShop.class).asList();
        System.out.printf("Number Of Shops: %d%n", coffeeShops.size());
        System.out.println("Warming Up...");
        warmupCount(100, coffeeShops);

        long totalTime = 0;
        for (int i = 0; i < 50; i++) {
            System.out.printf("--- Run: %d ---%n", i);

            // when
            long startTime = System.currentTimeMillis();
            long numberOfShops = coffeeShops.stream().count();
            long endTime = System.currentTimeMillis();

            // then
            long timeTaken = endTime - startTime;
            System.out.printf("Time Taken: %d millis%n", timeTaken);
            System.out.printf("There were %d shops%n", numberOfShops);
            totalTime += timeTaken;
        }
        System.out.printf("Mean time taken: " + totalTime / 100);
    }

    @Test
    public void shouldPerformCountBetterWhenParallel() {
        // given
        List<CoffeeShop> coffeeShops = datastore.find(CoffeeShop.class).asList();
        System.out.printf("Number Of Shops: %d%n", coffeeShops.size());
        System.out.println("Warming Up...");
        warmupCountParallel(100, coffeeShops);

        long totalTime = 0;
        for (int i = 0; i < 50; i++) {
            System.out.printf("--- Run: %d ---%n", i);

            // when
            long startTime = System.currentTimeMillis();
            long numberOfShops = coffeeShops.parallelStream().count();
            long endTime = System.currentTimeMillis();

            // then
            long timeTaken = endTime - startTime;
            System.out.printf("Time Taken: %d millis%n", timeTaken);
            System.out.printf("There were %d shops%n", numberOfShops);
            totalTime += timeTaken;
        }
        System.out.printf("Mean time taken: " + totalTime / 100);
    }

    private void warmupMatch(final int numberOfIterations, final List<CoffeeShop> coffeeShops) {
        for (int i = 0; i < numberOfIterations; i++) {
            coffeeShops.stream().anyMatch((coffeeShop) -> coffeeShop.getName().equals("Starbucks"));
        }
        System.gc();
        System.gc();
    }

    private void warmupMatchParallel(final int numberOfIterations, final List<CoffeeShop> coffeeShops) {
        for (int i = 0; i < numberOfIterations; i++) {
            coffeeShops.parallelStream().anyMatch((coffeeShop) -> coffeeShop.getName().equals("Starbucks"));
        }
        System.gc();
        System.gc();
    }

    private void warmupCount(final int numberOfIterations, final List<CoffeeShop> coffeeShops) {
        for (int i = 0; i < numberOfIterations; i++) {
            coffeeShops.stream().count();
        }
        System.gc();
        System.gc();
    }

    private void warmupCountParallel(final int numberOfIterations, final List<CoffeeShop> coffeeShops) {
        for (int i = 0; i < numberOfIterations; i++) {
            coffeeShops.parallelStream().count();
        }
        System.gc();
        System.gc();
    }

    private void warmupMap(final int numberOfIterations, final List<CoffeeShop> coffeeShops) {
        for (int i = 0; i < numberOfIterations; i++) {
            coffeeShops.stream()
                       .map(CoffeeShop::getName)
                       .collect(Collectors.toList());
        }
        System.gc();
        System.gc();
    }

    private void warmupMapParallel(final int numberOfIterations, final List<CoffeeShop> coffeeShops) {
        for (int i = 0; i < numberOfIterations; i++) {
            coffeeShops.parallelStream()
                       .map(CoffeeShop::getName)
                       .collect(Collectors.toList());
        }
        System.gc();
        System.gc();
    }

}
