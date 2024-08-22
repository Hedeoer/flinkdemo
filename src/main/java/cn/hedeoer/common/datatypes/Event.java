package cn.hedeoer.common.datatypes;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Random;



public class Event {
    public String user;
    public String url;
    public Long timestamp;
    public static final Instant START = Instant.parse("2020-01-01T12:00:00.000Z");

    public Event() {
        this.user = generateRandomUsername();
        this.url = generateRandomUrl();
        this.timestamp = START.toEpochMilli();
    }

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }


    // 生成随机的用户名（字符串类型）
    public String generateRandomUsername() {
        // 使用当前时间毫秒数作为基本部分
        long currentTimeMillis = System.currentTimeMillis();

        // 生成一个0到999之间的随机数
        Random random = new Random();
        int randomNumber = random.nextInt(100);

        // 拼接成一个唯一的用户名
        return "User_" + currentTimeMillis + "_" + randomNumber;
    }

// 生成随机的http链接
    /**
     * 生成随机的HTTP链接
     * @return 随机生成的HTTP链接
     */
    public String generateRandomUrl() {
        // 基础URL
        String base = "http://example.com/";

        // 生成随机路径
        Random random = new Random();
        int pathLength = random.nextInt(5) + 5; // 生成5到9长度的路径
        StringBuilder path = new StringBuilder();
        for (int i = 0; i < pathLength; i++) {
            char c = (char) ('a' + random.nextInt(26));
            path.append(c);
        }

        // 生成随机查询参数
        StringBuilder query = new StringBuilder();
        int queryLength = random.nextInt(5) + 5; // 生成5到9长度的查询参数
        for (int i = 0; i < queryLength; i++) {
            char c = (char) ('a' + random.nextInt(26));
            query.append(c);
        }

        try {
            URL url = new URL(base + path.toString() + "?query=" + query.toString());
            return url.toString();
        } catch (MalformedURLException e) {
            // 处理异常情况
            System.err.println("Failed to create URL: " + e.getMessage());
            return null;
        }
    }
}
