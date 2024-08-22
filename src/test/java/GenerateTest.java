import cn.hedeoer.common.utils.DataGenerator;

public class GenerateTest {
    public static void main(String[] args) {
        DataGenerator dataGenerator = new DataGenerator(1);
        dataGenerator.passengerCnt();
        String[] passengerNames = dataGenerator.getPassengerNames();
        for (int i = 0; i < passengerNames.length; i++) {
            System.out.println(passengerNames[i]);
        }
    }
}
