package cn.hedeoer.common.utils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class TimeFormat {
    public static String longToString(long time)
    {
        ZoneId zoneId = ZoneId.of("Asia/Shanghai");
        Instant instant = Instant.ofEpochMilli(time);
        ZonedDateTime zonedDateTime = instant.atZone(zoneId);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String result = zonedDateTime.format(formatter);
        return result;
    }

    public static void main(String[] args) {
        System.out.println(TimeFormat.longToString(1723095813709l));
    }
}
