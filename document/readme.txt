결과물 안내

[디렉토리]
conf
    : 카프카 및 java util 로그 설정 파일 위치
    : 로깅을 위해  -Djava.util.logging.config.file=${logging.properties위치} 필수
document
    : 프로젝트 관련 문서
log
    : 파일로그를 남기는 폴더 logging.properties에 설정
src
    : 소스 코드

[패키지]

>> main
com.kakaobank.etl : etl 기능을 가진 클래스 패키지
    DistinctTransfer
        : 중복제거를 Executor 를 이용한 쓰레드 관리로 병렬 처리를 구현하고자 하였으나
        : 메모리큐를 사용한 SimpleSource에서는 작동하였으나 카프카 소스에서는 실패
    SimpleTransfer
        : Executor를 이용한 쓰레드 관리와 synchronized로 공유 자원을 관리한
        : 멀티스레드 방식의 단순 데이터이동 클래스
    SingleSimpleTransfer
        : 단일 클래스 방식의 데이터 이동과 중복 제거 클래스 마찬가지로 카프카 소스에서는 실패
com.kakaobank.source : 데이터 소스 관련 클래스 패키지
    Source
        : 확장석을 높이기 위해 Source의 기본기능을 가진 인터페이스로 제네릭타입을 통해 확장성을 높임
    SimpleSource
        : 메모리 큐를 사용하는 단순 저장소
    KafkaSource
        : 카프카 Consumer/Producer를 사용하는 저장소
com.kakaobank.source.record : 데이터 소스의 레코드 관련 클래스 패키지
    PairRecord
        : 키/밸류 구조의 데이터를 다루기 위한 인터페이스
    SimplePairRecord
        : 단순 키/밸류의 구조를 구현한 제네릭 타입의 클래스
com.kakaobank.util : 유틸리티 클래스 패키지
    KafkaFactory
        : 카프카의 관련 객체들을 생성하고자 분리한 카프카 팩토리클래스
    LoggerFactory
        : 자체 로그 관리를 위해 분리한 로그 팩토리 클래스

>> test
com.kakaobank : 테스트 실행 클래스 패키지

DistinctTransferTestAppMain : 멀티스레드 중복 제거 관련 테스트
KafkaSourceTestAppMain : 멀티스레드 카프카 데이터 소스 관련 테스트
kafkaTransferTestAppMain : 멀티스레드 카프카 데이터 이동 관련 테스트
RecordTestAppMain : 멀티스레드 데이터 소스의 레코드 관련 테스트
simpleSourceTestAppMain : 멀티스레드 메모리큐 데이터 소스 관련 테스트
SimpleTransferTestAppMin : 멀티스레드 메모리큐 데이터 이동 관련 테스트
singleSimpleTransferTestAppMain : 데이터 이동관련 싱글 스레드 테스트
TestHelper : 테스트 유틸 클래스




