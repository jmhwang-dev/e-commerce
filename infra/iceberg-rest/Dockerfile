FROM apache/iceberg-rest-fixture:1.9.1

USER root

# PostgreSQL JDBC 드라이버 추가
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
    /usr/lib/iceberg-rest/

RUN chmod 644 /usr/lib/iceberg-rest/postgresql-42.7.3.jar

# ENTRYPOINT에서 -jar 대신 -cp 방식 사용
ENTRYPOINT ["java", "-cp", "/usr/lib/iceberg-rest/*", "org.apache.iceberg.rest.RESTCatalogServer"]

USER iceberg
