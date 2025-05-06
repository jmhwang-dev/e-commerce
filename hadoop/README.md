# SETUP
1. `/etc/hosts`에 클러스터 내 모든 노드들의 ip 명시
    - 중복 이름이 없는지 확인
    - master는 `core-site.xml`에 작성
2. ufw 방화벽 확인
    - master node <-> data nodes
    - data nodes <-> data nodes
3. ssh 연결 확인
    - master node <-> data nodes
    - data nodes <-> data nodes
4. `workers` 작성

# RUN TEST
1. `hdfs namenode -format`
2. `start-dfs.sh`
3. `hdfs dfs -mkdir /input`
4. `hdfs dfs -put file_path /input`
5. `dfs.datanode.data.dir`/current/BP-*/current/finalized 하위에 파일 생성되면 성공