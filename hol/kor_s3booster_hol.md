# s3booster-snowball Hands on Lab (Korean)

이 HoL은 Amazon S3 또는 Snowball Edge에 파일을 효율적으로 저장한기 위한 교육 목적입니다.
s3booster-snowball은 작은 파일(<1MB)을 1GB이상의 tar 파일로 묶어 네트워크 전속속도를 빠르게 해줍니다.

## 실습 설명
EFS 볼륨에 적재된 샘플 데이터를 aws s3 sync 명령으로 동기화할 때와, s3booster를 사용해 업로드할때 각각의 속도를 측정하고 성능을 비교합니다. 그리고 s3booster로 upload된 tar파일을 다운로드하여 원본 파일과 개수 및 크기를 비교합니다.

## 실습과정
### infra 환경준비
1. 인스턴스 생성
2. EFS 생성
3. EFS 파일 마운트
4. 테스트용 s3 버킷 생성

### s3booster 실행 환경 준비
1. python3 설치
2. s3booster 다운로드
   - git@github.com:aws-samples/s3booster-snowball.git
3. Sample Dataset 다운로드
   - [sample dataset](https://bit.ly/3KyQZbE)
4. EFS 마운팅 포인트에서 dataset 적재

### 파일업로드
1. s3booster로 파일 업로드
  - run.sh 참고
  - 업로드시 소요시간 측정
2. 파일시 업로드 경로 변경

### 성능 측정
1. aws cli 명령으로 테스트 데이터셋 업로드
2. 위 s3booster 업로드 측정값과 비교

## 생각할 거리
- 이 환경에서 병목현상이 발생할 가능성이 있는 요소는 어떤게 있을까요?
