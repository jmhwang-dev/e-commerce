# Build container image for spark-hadoop
```bash
cd ./scripts/spark && bash ./build.sh
```

# HOW TO SETUP ghrc
> 현재 시점(2025년 5월) 기준으로, GitHub Container Registry(GHCR)에 컨테이너 이미지를 푸시하거나 풀(Pull)하기 위해서는 `클래식 Personal Access Token(PAT)`을 사용하는 것이 권장된다. <br> 왜냐하면 `Fine-grained Personal Access Token(FGPT)`이 GHCR에 대한 인증을 완전히 지원하지 않기 때문

## 클래식 PAT 생성
- GitHub에서 Personal Access Tokens (classic) 페이지로 이동: https://github.com/settings/tokens

- Generate new token (classic)을 클릭

- 필요한 권한을 선택
    - read:packages: 패키지 읽기 권한
    - write:packages: 패키지 쓰기 권한
    - delete:packages: 패키지 삭제 권한 (필요한 경우)