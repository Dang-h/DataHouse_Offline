package com.atguigu.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

//将启动日志和事件日志区分开来，方便发往Kafka的不同Topic
public class LogTypeInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        // 区分日志类型：   body  header
        // 1 获取body数据
        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("UTF-8"));

        // 2 获取header
        Map<String, String> headers = event.getHeaders();

        // 3 判断数据类型并向Header中赋值
        if (log.contains("start")) {
            headers.put("topic","topic_start");
        }else {
            headers.put("topic","topic_event");
        }

        return event;

    }

    @Override
    public List<Event> intercept(List<Event> events) {

        ArrayList<Event> interceptors = new ArrayList<>();

        for (Event event : events) {
            Event intercept1 = intercept(event);

            interceptors.add(intercept1);
        }

        return interceptors;

    }

    @Override
    public void close() {

    }

    public static class Builder implements  Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }

//    //测试
//    public static void main(String[] args) {
//
//        //事件日志，标签event
//        Event event = EventBuilder.withBody("1562330043354|{\"cm\":{\"ln\":\"-49.9\",\"sv\":\"V2.7.1\",\"os\":\"8.1.4\",\"g\":\"44Q0K147@gmail.com\",\"mid\":\"999\",\"nw\":\"3G\",\"l\":\"es\",\"vc\":\"10\",\"hw\":\"1080*1920\",\"ar\":\"MX\",\"uid\":\"999\",\"t\":\"1562241570252\",\"la\":\"-51.8\",\"md\":\"sumsung-5\",\"vn\":\"1.2.7\",\"ba\":\"Sumsung\",\"sr\":\"K\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1562270188026\",\"en\":\"display\",\"kv\":{\"goodsid\":\"234\",\"action\":\"2\",\"extend1\":\"1\",\"place\":\"5\",\"category\":\"32\"}},{\"ett\":\"1562280043716\",\"en\":\"ad\",\"kv\":{\"entry\":\"1\",\"show_style\":\"5\",\"action\":\"5\",\"detail\":\"\",\"source\":\"1\",\"behavior\":\"1\",\"content\":\"1\",\"newstype\":\"2\"}},{\"ett\":\"1562329802951\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1562268245552\",\"action\":\"2\",\"type\":\"2\",\"content\":\"\"}},{\"ett\":\"1562309876441\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"2\"}},{\"ett\":\"1562284590285\",\"en\":\"comment\",\"kv\":{\"p_comment_id\":0,\"addtime\":\"1562250887080\",\"praise_count\":212,\"other_id\":0,\"comment_id\":3,\"reply_count\":48,\"userid\":3,\"content\":\"\"}},{\"ett\":\"1562262411144\",\"en\":\"favorites\",\"kv\":{\"course_id\":1,\"id\":0,\"add_time\":\"1562240275659\",\"userid\":0}},{\"ett\":\"1562294159879\",\"en\":\"praise\",\"kv\":{\"target_id\":9,\"id\":2,\"type\":4,\"add_time\":\"1562271042699\",\"userid\":9}}]}\n".getBytes());
//        Event event1 = EventBuilder.withBody("1562043354|{\"cm\":{\"ln\":\"-49.9\",\"sv\":\"V2.7.1\",\"os\":\"8.1.4\",\"g\":\"44Q0K147@gmail.com\",\"mid\":\"999\",\"nw\":\"3G\",\"l\":\"es\",\"vc\":\"10\",\"hw\":\"1080*1920\",\"ar\":\"MX\",\"uid\":\"999\",\"t\":\"1562241570252\",\"la\":\"-51.8\",\"md\":\"sumsung-5\",\"vn\":\"1.2.7\",\"ba\":\"Sumsung\",\"sr\":\"K\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1562270188026\",\"en\":\"display\",\"kv\":{\"goodsid\":\"234\",\"action\":\"2\",\"extend1\":\"1\",\"place\":\"5\",\"category\":\"32\"}},{\"ett\":\"1562280043716\",\"en\":\"ad\",\"kv\":{\"entry\":\"1\",\"show_style\":\"5\",\"action\":\"5\",\"detail\":\"\",\"source\":\"1\",\"behavior\":\"1\",\"content\":\"1\",\"newstype\":\"2\"}},{\"ett\":\"1562329802951\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1562268245552\",\"action\":\"2\",\"type\":\"2\",\"content\":\"\"}},{\"ett\":\"1562309876441\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"2\"}},{\"ett\":\"1562284590285\",\"en\":\"comment\",\"kv\":{\"p_comment_id\":0,\"addtime\":\"1562250887080\",\"praise_count\":212,\"other_id\":0,\"comment_id\":3,\"reply_count\":48,\"userid\":3,\"content\":\"\"}},{\"ett\":\"1562262411144\",\"en\":\"favorites\",\"kv\":{\"course_id\":1,\"id\":0,\"add_time\":\"1562240275659\",\"userid\":0}},{\"ett\":\"1562294159879\",\"en\":\"praise\",\"kv\":{\"target_id\":9,\"id\":2,\"type\":4,\"add_time\":\"1562271042699\",\"userid\":9}}]}\n".getBytes());
//
//        //启动日志，标签start
//        Event event2 = EventBuilder.withBody("{\"action\":\"1\",\"ar\":\"MX\",\"ba\":\"Sumsung\",\"detail\":\"433\",\"en\":\"start\",\"entry\":\"1\",\"extend1\":\"\",\"g\":\"N3K7KJR7@gmail.com\",\"hw\":\"750*1134\",\"l\":\"zh\",\"la\":\"5.1\",\"ln\":\"-104.9\",\"loading_time\":\"5\",\"md\":\"sumsung-13\",\"mid\":\"998\",\"nw\":\"WIFI\",\"open_ad_type\":\"2\",\"os\":\"8.0.1\",\"sr\":\"D\",\"sv\":\"V2.8.2\",\"t\":\"1562311120845\",\"uid\":\"998\",\"vc\":\"8\",\"vn\":\"1.1.0\"}\n".getBytes());
//        Event event2No = EventBuilder.withBody("{\"action\":\"1\",\"ar\":\"MX\",\"ba\":\"Sumsung\",\"detail\":\"433\",\"en\":\"\",\"entry\":\"1\",\"extend1\":\"\",\"g\":\"N3K7KJR7@gmail.com\",\"hw\":\"750*1134\",\"l\":\"zh\",\"la\":\"5.1\",\"ln\":\"-104.9\",\"loading_time\":\"5\",\"md\":\"sumsung-13\",\"mid\":\"998\",\"nw\":\"WIFI\",\"open_ad_type\":\"2\",\"os\":\"8.0.1\",\"sr\":\"D\",\"sv\":\"V2.8.2\",\"t\":\"1562311120845\",\"uid\":\"998\",\"vc\":\"8\",\"vn\":\"1.1.0\"}\n".getBytes());
//
//
//        LogTypeInterceptor logTypeInterceptor = new LogTypeInterceptor();
//        LogETLInterceptor logETLInterceptor = new LogETLInterceptor();
//
//        System.out.println(logTypeInterceptor.intercept(event));	//正常
//        System.out.println(logTypeInterceptor.intercept(event1));	//正常
//        System.out.println(logTypeInterceptor.intercept(event2));	//正常
//        System.out.println(logTypeInterceptor.intercept(event2No));	//正常
//        System.out.println("=================================");
//        System.out.println(logETLInterceptor.intercept(event));     //正常
//        System.out.println(logETLInterceptor.intercept(event1));    //时间戳错误
//        System.out.println(logETLInterceptor.intercept(event2));    //正常
//        System.out.println(logETLInterceptor.intercept(event2No));  //类型错误
//    }
}
