package com.kakaobank.pretest.application.record;

import com.kakaobank.pretest.framework.record.Pair;
import com.kakaobank.pretest.framework.record.PairRecord;

/**
 * event record를 처리하기 위한 데이터 객체
 * pair 인터페이스를 상속함으로 pair 타입으로 이루어진 frame워크를 적용
 *
 */
public class EventRecord implements Pair<String, String> {
    private final long eventID;
    private final String eventTimestamp;
    private final String serviceCode;
    private final String eventContext;

    public long getEventID() {
        return eventID;
    }

    public String getEventTimestamp() {
        return eventTimestamp;
    }

    public String getServiceCode() {
        return serviceCode;
    }

    public String getEventContext() {
        return eventContext;
    }

    public EventRecord(long eventID, String eventTimestamp, String serviceCode, String eventContext) {
        this.eventID = eventID;
        this.eventTimestamp = eventTimestamp;
        if(serviceCode.equals("") || serviceCode == null) {
            this.serviceCode = "null";
        } else {
            this.serviceCode = serviceCode;
        }
        if(eventContext.equals("") || eventContext == null) {
            this.eventContext = "null";
        } else {
            this.eventContext = eventContext;
        }
    }

    public EventRecord(PairRecord<String, String> pairRecord) {
        final String[] keySplit = pairRecord.getKey().split("\t");
        final String[] valueSplit = pairRecord.getValue().split("\t");

        this.eventID = Long.valueOf(keySplit[0]);
        this.eventTimestamp = keySplit[1];

        if(valueSplit.length == 2) {
            this.serviceCode = valueSplit[0];
            this.eventContext = valueSplit[1];
        } else if(valueSplit.length == 1) {
            this.serviceCode = valueSplit[0];
            this.eventContext = "null";
        } else {
            this.serviceCode = "null";
            this.eventContext = "null";
        }
    }

    public PairRecord<String, String> toPairRecord() {
        return new PairRecord(this.getKey(), getValue());
    }

    @Override
    public String getKey() {
        return new StringBuffer()
                .append(eventID).append("\t")
                .append(eventTimestamp)
                .toString();
    }

    @Override
    public String getValue() {
        return new StringBuffer()
                .append(eventID).append("\t")
                .append(eventTimestamp).append("\t")
                .append(serviceCode).append("\t")
                .append(eventContext)
                .toString();
    }

    @Override
    public String mkString() {
        return this.getKey() + "," + this.getValue();
    }

    @Override
    public String mkString(String s) {
        return this.getKey() + s + this.getValue();
    }

    @Override
    public boolean equals(Pair pair) {
        if(this.getKey().equals(pair.getKey()) == false)
            return false;
        else if(this.getValue().equals(pair.getValue()) == false)
            return false;
        else
            return true;
    }

    public String toString() {
        return this.mkString();
    }
}
