package com.kakaobank.pretest.application.record;

import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import com.kakaobank.pretest.application.TestHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

public class EventRecordDAOTest {
    private DBConfigurationBuilder configBuilder;
    private DB db;
    private Properties mysqlProps;
    private TestHelper testHelper = TestHelper.getInstance();

    @Before
    public void setUp() throws Exception {
        testHelper.deleteAllFiles("tmp/tmp_mysql_dir");

        DBConfigurationBuilder configBuilder = DBConfigurationBuilder.newBuilder()
                .setPort(3306)
                .setBaseDir("tmp/tmp_mysql_dir");

        DB db = DB.newEmbeddedDB(configBuilder.build());

        db.start();
        db.createDB("kakaobank-pretest");
        db.source("create-table.sql");
    }

    @After
    public void tearDown() throws Exception {
        testHelper.deleteAllFiles("tmp/tmp_mysql_dir");
    }

    @Test
    public void addAndGet() throws Exception {
        mysqlProps = testHelper.readProps("conf/test/test-mysql.properties");

        EventRecord record1 = new EventRecord((long) 1,
                "eventTimeStamp" + 1,
                "serviceCode" + 1,
                "eventContext" + 1);

        EventRecord record2 = new EventRecord((long) 2,
                "eventTimeStamp" + 2,
                "serviceCode" + 2,
                "eventContext" + 2);

        EventRecordDAO dao = new EventRecordDAO(mysqlProps);

        assertTrue(record1.equals(record1));
        assertFalse(record1.equals(record2));

        dao.add(record1);
        List<EventRecord> readRecords = (dao.get((long) 1, "eventTimeStamp" + 1));
        EventRecord record = readRecords.get(0);
        assertTrue(record.equals(record1));

        assertEquals(dao.get((long) 1, "eventTimeStamp" + 1).size(), 1);
        dao.add(record1);
        assertEquals(dao.get((long) 1, "eventTimeStamp" + 1).size(), 2);
        dao.add(record2);
        assertEquals(dao.get((long) 1, "eventTimeStamp" + 1).size(), 2);
        assertEquals(dao.get((long) 2, "eventTimeStamp" + 2).size(), 1);
    }
}