# 이커머스 데이터 플랫폼

이 저장소는 Olist 브라질 전자상거래 데이터셋을 활용한 데이터 플랫폼과 분석 환경 구축 예제를 포함합니다.

- `infra/` - 인프라 코드 모음
  - `k8s/` - MinIO와 Spark 배포용 Kubernetes 매니페스트와 스크립트
  - `ansible/` - 노드 프로비저닝용 플레이북
  - `hadoop/` - HDFS 클러스터 설정 파일
- `scripts/` - 로컬 개발을 위한 유틸리티 스크립트
- `src/` - 추론 파이프라인 등 애플리케이션 코드
- `notebooks/` - 탐색적 데이터 분석 노트북
- `docs/` - ER 다이어그램 등 프로젝트 문서

## 빠른 시작

`infra/k8s/scripts` 폴더에는 Helm을 이용해 로컬 환경에 MinIO와 Spark 스택을 배포하는 스크립트가 있습니다. 먼저 `.env.example`을 `infra/k8s/.env`로 복사한 뒤 자격 정보를 설정하세요.

```bash
cp .env.example infra/k8s/.env
cd infra/k8s/scripts
./deploy_all.sh
```
배포가 완료되면 `http://localhost:30901`에서 MinIO 콘솔에 접속할 수 있으며, 계정 정보는 `.env` 파일에 작성한 값을 사용합니다.

추가적인 kubectl 및 helm 명령어는 [docs/commands.md](docs/commands.md)에서 확인할 수 있습니다.
