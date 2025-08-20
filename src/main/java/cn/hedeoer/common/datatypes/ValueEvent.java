package cn.hedeoer.common.datatypes;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

// 1. 定义一个 POJO 类来承载解析后的数据，方便后续处理
    public  class ValueEvent implements Serializable {
        private int key;
        private long timestamp;

        public ValueEvent() {}

        public ValueEvent(int key, long timestamp) {
            this.key = key;
            this.timestamp = timestamp;
        }

        public int getKey() {
            return key;
        }

        public void setKey(int key) {
            this.key = key;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        // 为了方便打印输出，重写 toString 方法
        @Override
        public String toString() {
            LocalDateTime ldt = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
            return "Event{" +
                    "key=" + key +
                    ", time=" + ldt.toLocalTime() +
                    '}';
        }
    }
