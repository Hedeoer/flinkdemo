package cn.hedeoer.chapter01.operations.datastream_transformations.watermark;

/**
 * 水印的生成基于事件时间
 * 需要考虑的问题：
 * 1. 乱序数据如何处理
 *
 * 2. 上游是多并行度的task对来自多个上游task的水印如何取舍
 *  取上游多个水印中最小的水印作为当前task的水印
 *
 * 3. 水印如何生成
 *  ①当前数据流是有序的，还是乱序的
 *  ②水印的提取
 *  ③水印的发送的周期
 *      周期新的，间隔性的
 *
 * 4. flink内置的水印策略
 *  forMonotonousTimestamps，水印单调递增，发送周期为200ms
 *  forBoundedOutOfOrderness 水印允许乱序，可以设置最大的乱序程度
 *
 *
 * 5.自定义水印的生成策略
 *  实现WatermarkGenerator接口
 *
 * 6. 水印的分发位置
 *  离数据源越近越好，甚至可以在source算子中生成水印
 */
public class $15WatermarkGenerator {
    public static void main(String[] args) {

    }
}
