package cn.hedeoer.common.datatypes;

public class Item {
    private final String id;
    private final Shape shape;
    private final String color;

    public Item(String id, Shape shape, String color) {
        this.id = id;
        this.shape = shape;
        this.color = color;
    }

    public Shape getShape() {
        return shape;
    }

    @Override
    public String toString() {
        return "Item{" +
                "id='" + id + '\'' +
                ", shape=" + shape +
                ", color='" + color + '\'' +
                '}';
    }

    public String getColor() {
        return  this.color;
    }
}