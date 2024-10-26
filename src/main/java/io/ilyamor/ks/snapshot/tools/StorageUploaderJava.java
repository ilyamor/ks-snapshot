package io.ilyamor.ks.snapshot.tools;

import org.apache.kafka.streams.state.internals.OffsetCheckpoint;

import java.io.File;
import java.io.InputStream;
import java.util.Properties;

public interface StorageUploaderJava {
    OffsetCheckpoint getCheckpointFile(String partition, String storeName, String applicationId);
    InputStream getStateStores(String partition, String storeName, String applicationId, String offset);
    Threeple uploadStateStore(File archiveFile, File checkPoint);
    StorageUploaderJava configure(Properties params, String storeName);

    class Threeple {
        private final String pathToArchive;
        private final String pathToCheckpoint;
        private final long timeOfUploading;

        public Threeple(String pathToArchive, String pathToCheckpoint, long timeOfUploading) {
            this.pathToArchive = pathToArchive;
            this.pathToCheckpoint = pathToCheckpoint;
            this.timeOfUploading = timeOfUploading;
        }

        public String getPathToArchive() {
            return pathToArchive;
        }

        public String getPathToCheckpoint() {
            return pathToCheckpoint;
        }

        public long getTimeOfUploading() {
            return timeOfUploading;
        }
    }
}
