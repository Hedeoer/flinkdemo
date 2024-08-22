package cn.hedeoer.common.sources;

import cn.hedeoer.common.datatypes.VideoPlayProgress;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class VideoPlayProgressSource implements SourceFunction<VideoPlayProgress> {

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<VideoPlayProgress> ctx) throws Exception {
        Random random = new Random();

        while (isRunning) {
            // 模拟生成数据
            long playSec = random.nextInt(3600); // 随机播放秒数，假设最大为3600秒
            long positionSec = random.nextInt(3600); // 随机位置秒数，假设最大为3600秒
            long videoId = random.nextLong() & Long.MAX_VALUE; // 随机视频ID

            VideoPlayProgress progress = new VideoPlayProgress(playSec, positionSec, videoId);
            ctx.collect(progress);

            // 控制数据生成速率
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
