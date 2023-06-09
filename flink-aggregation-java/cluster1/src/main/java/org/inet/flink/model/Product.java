package org.inet.flink.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

// {"id": 1, "name": "Apple", "price": 0.85}

public class Product {
    private final Long id;
    private String name;
    private Double price;
    private static final Random RANDOM = new Random();

    public Product() {
        List<String> names = listOfNames();
        List<String> prices = listOfPrices();

        this.id = System.currentTimeMillis();
        this.name = names.get(RANDOM.nextInt(names.size()));
        this.price = Double.parseDouble(prices.get(RANDOM.nextInt(prices.size())));
    }

    public Product(Long id, String name, Double price) {
        this.id = id;
        this.name = name;
        this.price = price;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Double getPrice() {
        return price;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setPrice(Double price) {
        this.price = price;
    }
    
    @Override
    public String toString() {
        return "Product{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", price=" + price +
                '}';
    }

    private static List<String> listOfNames() {
        List<String> names = new ArrayList<>();
        names.add("Apple");
        names.add("Melon");
        names.add("Lemon");
        return names;
    }

    private static List<String> listOfPrices() {
        List<String> prices = new ArrayList<>();
        prices.add("1.05");
        prices.add("0.80");
        prices.add("0.55");
        return prices;
    }
}
