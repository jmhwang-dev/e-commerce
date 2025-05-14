# INSTALL
```bash
bash ./hadoop/install/run.sh
```

# SETUP
1. `/etc/hosts` 작성
    - master node에 클러스터 내 모든 노드들의 ip 작성
    - worker node에 마스터 노드의 ip 작성
    - 중복 이름이 없는지 확인
    - master 호스트를 `core-site.xml`에 작성
    - [os가 ubuntu server라면]
        - `/etc/cloud/cloud.cfg`에서 `update_etc_hosts` 주석 (들여쓰기 주의)

2. ufw 방화벽 확인
    - master node <-> data nodes
    - data nodes <-> data nodes
3. ssh 연결 확인
    - master node <-> data nodes
    - data nodes <-> data nodes
4. `workers` 파일에 `/etc/hosts`에 작성한 워커 이름 작성

# RUN TEST
```bash
hdfs namenode -format
start-dfs.sh
hdfs dfs -mkdir /input
hdfs dfs -put file_path /input
```

`dfs.datanode.data.dir`/current/BP-*/current/finalized 하위에 파일 생성되면 성공