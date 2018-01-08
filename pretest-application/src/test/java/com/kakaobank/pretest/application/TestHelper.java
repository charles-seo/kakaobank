package com.kakaobank.pretest.application;

import com.kakaobank.pretest.application.record.EventRecord;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Application 테스트 Help 객체
 */
public class TestHelper {
    private static final Logger logger = Logger.getLogger(TestHelper.class.getName());
    private static final TestHelper instance = new TestHelper();

    public static TestHelper getInstance() {
        return instance;
    }

    private TestHelper() {
    }

    public List<EventRecord> createSampleEventRecord(int recordCount) {
        List<EventRecord> samplePairRecord = new LinkedList<>();

        for (int i = 0; i < recordCount; i++) {
            samplePairRecord.add(new EventRecord(
                    (long) i,
                    "eventTimeStamp" + i,
                    "serviceCode" + i,
                    "eventContext" + i));
        }

        return samplePairRecord;
    }

    public Properties readProps(String propFile) {
        Properties prop = new Properties();
        InputStream input = null;

        try {
            input = new FileInputStream(propFile);
            prop.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return prop;
    }

    public boolean allFuterIsDone(List<Future> futureList) {
        boolean result = true;
        for(Future future : futureList) {
            if(future.isDone() == false) {
                result = false;
            }
        }
        return result;
    }

    public synchronized void deleteAllFiles(String path) {
        File file = new File(path);
        File[] tempFiles = file.listFiles();

        if ((tempFiles != null) && (tempFiles.length > 0)) {
            for (File tempFile : tempFiles) {
                if (tempFile.isFile()) {
                    tempFile.delete();
                } else {
                    deleteAllFiles(tempFile.getPath());
                }
                tempFile.delete();
            }
            file.delete();
        }
    }
}