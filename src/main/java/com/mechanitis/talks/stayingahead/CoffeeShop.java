package com.mechanitis.talks.stayingahead;

public class CoffeeShop {
    private Point location;
    private String name;

    public String getName() {
        return name;
    }

    public Point getLocation() {
        return location;
    }

    public static class Point {
        private double[] coordinates;

        public double[] getCoordinates() {
            return coordinates;
        }
    }
}
