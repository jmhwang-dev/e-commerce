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