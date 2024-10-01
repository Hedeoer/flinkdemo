package cn.hedeoer.common.sources;

import cn.hedeoer.common.datatypes.DataModel;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class MyAcSource extends RichParallelSourceFunction<DataModel> {

        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<DataModel> ctx) throws Exception {
            while (isRunning){
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                DataModel model = new DataModel();
                model.setSourceSubtaskNumber("SourceSubtaskNumber:"+indexOfThisSubtask);
                ctx.collect(model);
                Thread.sleep(500);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }