package org.dav;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Json {
    public static <T> T parse(String json, Class<T> clazz, boolean requireExpose) {
        GsonBuilder builder = new GsonBuilder();
        if (requireExpose) {
            builder.excludeFieldsWithoutExposeAnnotation();
        }
        Gson gson = builder.create();
        return gson.fromJson(json, clazz);
    }

    public static <T> String format(T json, boolean requireExpose) {
        GsonBuilder builder = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        if (requireExpose) {
            builder.excludeFieldsWithoutExposeAnnotation();
        }
        Gson gson = builder.create();
        return gson.toJson(json);
    }
}
