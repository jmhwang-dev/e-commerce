# 기본 설정
1. `/etc/hosts` 작성
    - 모든 node에 클러스터 내 모든 노드들의 ip 작성
        - ex) x.x.x.x nodename
    - 중복 이름이 없는지 확인
    - master 호스트를 `core-site.xml`에 작성
    - [os가 ubuntu server라면]
        - `/etc/cloud/cloud.cfg`에서 `update_etc_hosts` 주석 (들여쓰기 주의)
        
2. ufw 방화벽 확인
    - master node <-> worekr nodes
    - worker nodes <-> worker nodes
    - client <-> master node, worker nodes

3. ssh 연결 확인
    - master node <-> worker nodes
    - worker nodes <-> worker nodes
    - ~/.ssh/config에서 `IdentitiesOnly` `yes` 추가
        ```bash
        # 비인터렉티브 세션에서는 ssh agent는 실행되지 않음
        eval "$(ssh-agent -s)"
        ssh-add ~/.ssh/dev/jungmin.hwang.dev
        ```

4. `workers` 파일에 `/etc/hosts`에 작성한 워커 이름 작성

5. ~/.ssh/enviorment 내용 확인 -> `start-dfs.sh`를 사용하려면 반드시 필요

# 설치

## `pipx`
```bash
# 1. 사용자 홈 디렉토리에 pipx 설치
python3 -m pip install --user pipx

# 2. pipx 설치 경로를 PATH에 자동 추가
python3 -m pipx ensurepath

# 3. (필요 시) 셸 다시 시작하거나 source ~/.bashrc 등

# 4. pipx로 Ansible 설치
pipx install --include-deps ansible
```

## Frameworks
```bash
cd infra/ansible
ansible-playbook -i inventory.yml <playbook>.yml -K
```

# 테스트 실행
## HDFS
```bash
# 네임노드에서
hdfs namenode -format
start-dfs.sh    
hdfs dfs -mkdir /input
hdfs dfs -put file_path /input
```

1. `hdfs dfsadmin -report` 결과 확인: 모든 워커노드 연결 확인

2. `dfs.datanode.data.dir`/current/BP-*/current/finalized 하위에 파일 생성되면 성공

3. `hdfs dfs -cat /input/{file_path}`로 출력 확인

## 변수

### git_clone_path
플레이북이 위치한 경로를 기준으로 저장소 루트를 자동으로 결정합니다:

```yaml
git_clone_path: "{{ playbook_dir | dirname | dirname }}"
```

다른 위치에서 플레이북을 실행할 때는 다음과 같이 변수를 덮어쓸 수 있습니다:

```bash
ansible-playbook -i inventory.yml playbook_hdfs.yml -e git_clone_path=/path/to/repo
```
