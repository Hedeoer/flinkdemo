package cn.hedeoer.common.utils;

import cn.hedeoer.common.datatypes.WaterSensor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DataUtil {
    public static List<WaterSensor> elementToList(Iterable<WaterSensor> elements) {
        ArrayList<WaterSensor> list = new ArrayList<>();
        Iterator<WaterSensor> iterator = elements.iterator();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list;
    }
}
