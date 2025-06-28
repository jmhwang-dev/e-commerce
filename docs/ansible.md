# INSTALL

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
cd ./ansible
ansible-playbook -i inventory.yml {}.yml -K     # sudo 권한
```

## 다른 유용한 옵션들
```bash
# 상세한 출력
ansible-playbook playbook_hdfs.yml --check -v

# 변경사항만 표시
ansible-playbook playbook_hdfs.yml --check --diff

# 특정 태스크부터 시작
ansible-playbook playbook_hdfs.yml --check --start-at-task="Download hadoop"

# 특정 호스트만
ansible-playbook playbook_hdfs.yml --check --limit master
```