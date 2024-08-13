import cn.hedeoer.common.datatypes.Item;
import cn.hedeoer.common.datatypes.Rule;
import cn.hedeoer.common.datatypes.Shape;

public class ShapeTest {
    public static void main(String[] args) {
        Rule rule = new Rule("rule1", Shape.CIRCLE, Shape.SQUARE);
        Item red = new Item("1", Shape.CIRCLE, "red");

        System.out.println(rule.first.equals(red.getShape()));
    }
}
