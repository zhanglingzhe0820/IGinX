package cn.edu.tsinghua.iginx.mongodb.tools;

import cn.edu.tsinghua.iginx.exceptions.UnsupportedDataTypeException;
import cn.edu.tsinghua.iginx.mongodb.MongoDBStorage;
import cn.edu.tsinghua.iginx.mongodb.query.entity.MongoDBSchema;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.bson.Document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.edu.tsinghua.iginx.thrift.DataType.*;

public class DataUtils {

    public static String reformatPattern(String pattern) {
        pattern = pattern.replaceAll("[.]", "\\\\.");
        pattern = pattern.replaceAll("[*]", ".*");
        return pattern;
    }

    public static String toString(DataType dataType) {
        switch (dataType) {
            case INTEGER:
                return "int";
            case LONG:
                return "long";
            case BOOLEAN:
                return "bool";
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case BINARY:
                return "binary";
        }
        return "";
    }

    public static DataType fromString(String dataType) {
        switch (dataType) {
            case "bool":
                return BOOLEAN;
            case "int":
                return INTEGER;
            case "long":
                return LONG;
            case "float":
                return FLOAT;
            case "double":
                return DOUBLE;
            case "binary":
                return BINARY;
            default:
                throw new UnsupportedDataTypeException(dataType);
        }
    }

    public static Document constructDocument(MongoDBSchema schema, DataType type, List<JSONObject> jsonObjects) {
        Document document = new Document();
        document.append(MongoDBStorage.NAME, schema.getName());
        document.append(MongoDBStorage.TYPE, toString(type));
        for (Map.Entry<String, String> entry : schema.getTags().entrySet()) {
            document.append(MongoDBStorage.TAG_PREFIX + entry.getKey(), entry.getValue());
        }
        document.append(MongoDBStorage.FULLNAME, schema.getName() + "{" + schema.getTagsAsString() + "}");
        JSONArray jsonArray = new JSONArray();
        for (int i = 0; i < jsonObjects.size(); i++) {
            jsonArray.add(jsonObjects);
        }
        document.append(MongoDBStorage.VALUES, jsonArray.toString());
        return document;
    }

}
