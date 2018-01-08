package com.kakaobank.pretest.application.etl;

import com.kakaobank.pretest.application.record.EventRecordDAO;
import com.kakaobank.pretest.framework.source.KafkaSource;
import com.kakaobank.pretest.framework.util.KafkaFactory;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class KafkaToMysql {
    private static final Logger logger = Logger.getLogger(KafkaToMysql.class.getName());

    final KafkaSource fromKafkaSource;
    final EventRecordDAO toMysql;

    public KafkaToMysql(String fromKafkaTopic, Properties fromKafkaProps,Properties toMysqlProps) {
        fromKafkaSource = new KafkaSource(fromKafkaTopic, new KafkaFactory<String, String>(fromKafkaProps));
        toMysql = new EventRecordDAO(toMysqlProps);
    }

    public boolean move(int limitTimeSec, int intervalTimeSec) {
        final long limitTImeMillis = System.currentTimeMillis() + limitTimeSec * 1000;

        boolean result = true;
        long totalRecord = 0;
        do {

            try {
                final List readRecord = fromKafkaSource.read(intervalTimeSec);
                final int[] resultSet = toMysql.add(readRecord);

                logger.info("move kafka to mysql by interval time" + resultSet.length + "/" + readRecord.size());
                totalRecord = totalRecord + resultSet.length;
                if( resultSet.length != readRecord.size()) {
                    result = false;
                }
            } catch (SQLException | ClassNotFoundException e) {
                e.printStackTrace();
                logger.error("mysql insert faile.", e);
            }

        } while(System.currentTimeMillis() < limitTImeMillis);

        if(result) {
            logger.info("move from kafka to mysql complete : " + totalRecord);
        } else {
            logger.info("move from kafka to mysql complete with error : " + totalRecord);
        }
        return result;
    }
}
