FROM apache/iceberg-rest-fixture:1.9.1

USER root

# PostgreSQL JDBC 드라이버 추가
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
    /usr/lib/iceberg-rest/

ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    /usr/lib/iceberg-rest/

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    /usr/lib/iceberg-rest/

RUN chmod 644 /usr/lib/iceberg-rest/postgresql-42.7.3.jar
RUN chmod 644 /usr/lib/iceberg-rest/hadoop-aws-3.3.4.jar
RUN chmod 644 /usr/lib/iceberg-rest/aws-java-sdk-bundle-1.12.262.jar


# ENTRYPOINT에서 -jar 대신 -cp 방식 사용
ENTRYPOINT ["java", "-cp", "/usr/lib/iceberg-rest/*", "org.apache.iceberg.rest.RESTCatalogServer"]
