package io.ilyamor.ks.snapshot.tools;

public class UploaderUtils {
    private UploaderUtils() {}

    public static boolean isJavaStorageUploaderInstance(Class<?> clazz) {
        return StorageUploaderJava.class.isAssignableFrom(clazz);
    }
}
