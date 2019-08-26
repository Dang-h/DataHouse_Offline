package com.atguigu.flume.interceptor;

import org.apache.commons.lang.math.NumberUtils;

public class LogUtils {
    public static boolean validateStart(String log) {

        if (log==null) {
            return false;
        }

        if (!log.trim().startsWith("{") || !log.trim().endsWith("}")) {
            return false;
        }
        return true;
    }

    public static boolean validateEvent(String log) {

        if (log == null) {
            return false;
        }

        String[] split = log.split("\\|");

        if (split.length != 2) {
            return false;
        }
        if (split[0].trim().length() != 13 || !NumberUtils.isDigits(split[0].trim())) {
            return false;
        }
        if (!split[1].trim().startsWith("{") || !split[1].trim().endsWith("}")) {
            return false;
        }

        return true;
    }
}
