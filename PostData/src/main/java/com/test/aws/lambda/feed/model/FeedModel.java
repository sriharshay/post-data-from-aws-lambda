package com.test.aws.lambda.feed.model;

public class FeedModel {
    private String name;
    private String country;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    @Override
    public String toString() {
        return "FeedModel{" +
                "name='" + name + '\'' +
                ", country='" + country + '\'' +
                '}';
    }
}
