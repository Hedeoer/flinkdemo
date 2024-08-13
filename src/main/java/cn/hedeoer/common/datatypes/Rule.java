package cn.hedeoer.common.datatypes;

public class Rule {
    public final String name;
    public final Shape first;
    public final Shape second;

    public Rule(String name, Shape first, Shape second) {
        this.name = name;
        this.first = first;
        this.second = second;
    }

    @Override
    public String toString() {
        return "Rule{" +
                "name='" + name + '\'' +
                ", first=" + first +
                ", second=" + second +
                '}';
    }
}