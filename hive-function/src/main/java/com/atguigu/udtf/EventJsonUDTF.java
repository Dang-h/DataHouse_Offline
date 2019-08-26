package com.atguigu.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class EventJsonUDTF extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        List<? extends StructField> allStructFieldRefs = argOIs.getAllStructFieldRefs();

        if (allStructFieldRefs.size() != 1) {
            throw new UDFArgumentException("参数只能为1");
        }

        if (!"string".equals(allStructFieldRefs.get(0).getFieldObjectInspector().getTypeName())) {
            throw new UDFArgumentException("类型只能为string");
        }

        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();

        fieldNames.add("event_name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        fieldNames.add("event_json");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }


    @Override
    public void process(Object[] args) throws HiveException {

        String events = (String) args[0];

        if (StringUtils.isBlank(events)) {
            return;
        }

        try {
            JSONArray eventArray = new JSONArray(events);
            for (int i = 0; i < eventArray.length(); i++) {

                String[] result = new String[2];
                JSONObject event = eventArray.getJSONObject(i);

                result[0] = event.getString("en");
                result[1] = event.toString();

                forward(result);
            }

        } catch (JSONException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void close() throws HiveException {

    }
}
