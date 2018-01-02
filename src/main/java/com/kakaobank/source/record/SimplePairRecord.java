package com.kakaobank.source.record;

import com.kakaobank.util.LoggerFactory;

import java.util.logging.Logger;

/**
 * 단순 key-value 데이터를 저장하는데 사용하기 위한 객체
 *
 * @param <K> Key 타입
 * @param <V> Value 타입
 */
public class SimplePairRecord<K, V> implements PairRecord<K, V> {
    private final static Logger logger = LoggerFactory.getLogger("SimplePairRecord");

    private K key;
    private V value;

    public SimplePairRecord(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public K getKey() {
        return this.key;
    }

    @Override
    public V getValue() {
        return this.value;
    }

    /**
     * 구분자는 ','를 사용하여 스트링 문자열 생성
     * @return
     */
    @Override
    public String mkString() {
        return this.key.toString() + "," + this.value.toString();
    }

    /**
     * 구분자를 전달 받아 스트링 문자열 생성
     * @param delimiter
     * @return
     */
    @Override
    public String mkString(String delimiter) {
        return this.key.toString() + delimiter + this.value.toString();
    }

    /**
     * 값 비교 동등 메소드 생성
     *
     * @param pairRecord
     * @return
     */
    @Override
    public boolean equal(PairRecord<K, V> pairRecord) {

        if(this.key.equals(pairRecord.getKey()) != true) {
            return false;
        }
        else if(this.value.equals(pairRecord.getValue()) != true) {
            return false;
        }
        else
            return true;
    }

    /**
     * 묵시적 형변환을 위한 메소드 오버라이딩
     *
     * @return
     */
    public String toString() {
        return this.mkString();
    }
}
