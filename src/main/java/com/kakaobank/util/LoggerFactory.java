package com.kakaobank.util;

import java.util.logging.*;

/**
 * 로그 팩토리
 * 로그 생성 및 레벨 관리를 위한 클래스
 *
 */
public class LoggerFactory {
    private static final Logger logger = LoggerFactory.getLogger("LoggerFactory");
    private static LoggerFactory instance = new LoggerFactory();

    public static LoggerFactory getInstance() {
        return instance;
    }

    private LoggerFactory() {

    }

    public static Logger getLogger(String className) {
        return Logger.getLogger(className);
    }
}
