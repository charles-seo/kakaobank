package com.kakaobank.pretest.application.etl;

import com.kakaobank.pretest.application.record.EventRecordDAO;
import com.kakaobank.pretest.framework.etl.KafkaTransfer;
import com.kakaobank.pretest.framework.source.KafkaSource;
import com.kakaobank.pretest.framework.util.KafkaFactory;
import org.apache.log4j.Logger;

import java.util.Properties;

public class KafkaTransferWorker {
    private static final Logger logger = Logger.getLogger(KafkaTransferWorker.class.getName());

    private final Properties fromKafkaClientProps;
    private final Properties toKafkaClientProps;

    public KafkaTransferWorker(Properties fromKafkaClientProps, Properties toKafkaClientProps) {
        this.fromKafkaClientProps = fromKafkaClientProps;
        this.toKafkaClientProps = toKafkaClientProps;
    }

    public KafkaTransfer work(final String fromTopic, final String toTopic) {
        return this.getKafkaTransfer(fromTopic, toTopic);
    }

    private KafkaTransfer getKafkaTransfer(String fromTopic, String toTopic) {
        final KafkaTransfer kafkaTransfer = new KafkaTransfer(new KafkaSource<>(fromTopic, new KafkaFactory<>(fromKafkaClientProps)),
                new KafkaSource<>(toTopic, new KafkaFactory<>(toKafkaClientProps)));

        return kafkaTransfer;
    }
}
