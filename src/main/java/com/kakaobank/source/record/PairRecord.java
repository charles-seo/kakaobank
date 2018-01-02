package com.kakaobank.source.record;

/**
 * key-value 데이터를 사용하기 위한 인터페이스
 * @param <K> Key 타입
 * @param <V> Value 타입
 */
public interface PairRecord<K, V> {
//    void setKey(K key);
//    void setValue(V value);
    K getKey();
    V getValue();
    String mkString();
    String mkString(String delimiter);
    boolean equal(PairRecord<K, V> pairRecord);
}
