# Build container image for spark-hadoop
```bash
cd ./spark/image && bash ./build.sh
```

# HOW TO SETUP GHCR
현재 시점(2025년 5월) 기준으로 GitHub Container Registry(GHCR)에 컨테이너 이미지를 푸시하거나 받기 위해서는 "클래식" Personal Access Token(PAT)을 사용하는 것이 권장됩니다. Fine-grained PAT은 아직 GHCR 인증을 완전히 지원하지 않습니다.

## 클래식 PAT 생성
- GitHub에서 Personal Access Tokens (classic) 페이지로 이동: <https://github.com/settings/tokens>
- **Generate new token (classic)** 클릭
- 필요한 권한 선택
  - `read:packages`: 패키지 읽기
  - `write:packages`: 패키지 쓰기
  - `delete:packages`: 패키지 삭제 (선택)

