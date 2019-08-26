package com.atguigu.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class BaseFieldUDF extends UDF {

    public String evaluate(String log, String key) throws JSONException {
        String[] split = log.split("\\|");

        if (split.length != 2 || StringUtils.isBlank(split[1])) {
            return "";
        }

        JSONObject base = new JSONObject(split[1]);

        if ("et".equals(key)) {

            if (!base.has("et")) {
                return "";
            }

            String et = base.getString(key);
            if (et != null && !"".equals(et)) {
                return et;
            }
        } else if ("st".equals(key)) {
            return split[0];
        } else {
            if (!base.has("cm")) {
                return "";
            }

            JSONObject cm = base.getJSONObject("cm");
            if (cm.has(key)) {
                return cm.getString(key);
            }

        }
        return "";
    }

    public static void main(String[] args) throws JSONException {
        String eventLog = "11562380049049|{\"cm\":{\"ln\":\"-63.1\",\"sv\":\"V2.5.3\",\"os\":\"8.1.2\",\"g\":\"GMPOH51U@gmail.com\",\"mid\":\"10\",\"nw\":\"4G\",\"l\":\"pt\",\"vc\":\"6\",\"hw\":\"750*1134\",\"ar\":\"MX\",\"uid\":\"10\",\"t\":\"1562283301474\",\"la\":\"-26.2\",\"md\":\"sumsung-14\",\"vn\":\"1.2.4\",\"ba\":\"Sumsung\",\"sr\":\"R\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1562360036751\",\"en\":\"display\",\"kv\":{\"goodsid\":\"1\",\"action\":\"2\",\"extend1\":\"2\",\"place\":\"5\",\"category\":\"85\"}},{\"ett\":\"1562287356173\",\"en\":\"newsdetail\",\"kv\":{\"entry\":\"2\",\"goodsid\":\"2\",\"news_staytime\":\"6\",\"loading_time\":\"12\",\"action\":\"2\",\"showtype\":\"3\",\"category\":\"37\",\"type1\":\"\"}},{\"ett\":\"1562296950419\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"36\",\"action\":\"1\",\"extend1\":\"\",\"type\":\"3\",\"type1\":\"433\",\"loading_way\":\"1\"}},{\"ett\":\"1562292601263\",\"en\":\"ad\",\"kv\":{\"entry\":\"1\",\"show_style\":\"0\",\"action\":\"2\",\"detail\":\"\",\"source\":\"1\",\"behavior\":\"2\",\"content\":\"1\",\"newstype\":\"6\"}},{\"ett\":\"1562352092949\",\"en\":\"active_foreground\",\"kv\":{\"access\":\"\",\"push_id\":\"2\"}},{\"ett\":\"1562323227367\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"1\"}},{\"ett\":\"1562306572652\",\"en\":\"error\",\"kv\":{\"errorDetail\":\"java.lang.NullPointerException\\\\n    at cn.lift.appIn.web.AbstractBaseController.validInbound(AbstractBaseController.java:72)\\\\n at cn.lift.dfdf.web.AbstractBaseController.validInbound\",\"errorBrief\":\"at cn.lift.appIn.control.CommandUtil.getInfo(CommandUtil.java:67)\"}},{\"ett\":\"1562363767016\",\"en\":\"comment\",\"kv\":{\"p_comment_id\":1,\"addtime\":\"1562335965929\",\"praise_count\":32,\"other_id\":4,\"comment_id\":9,\"reply_count\":73,\"userid\":5,\"content\":\"突韦淌猫劣诸蹬常秋哆莱限\"}},{\"ett\":\"1562286911566\",\"en\":\"favorites\",\"kv\":{\"course_id\":4,\"id\":0,\"add_time\":\"1562365324118\",\"userid\":0}}]}";

        String result = new BaseFieldUDF().evaluate(eventLog, "et");
        System.out.println(result);
    }
}
