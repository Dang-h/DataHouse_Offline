package com.atguigu.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;

public class LogETLInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        //获取event的body
        byte[] body = event.getBody();
        //获取内容
        String log = new String(body, Charset.forName("UTF-8"));

        //日志类别筛选
        if (log.contains("start")) {
            if (LogUtils.validateStart(log)) {
                return event;
            }
        } else {
            if (LogUtils.validateEvent(log)) {
                return event;
            }
        }

        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        events.removeIf(event -> intercept(event) == null);
        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {

            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
