# Introduction

`.csv` 파일로부터 단체 번개톡을 전송하기 위한 저장소입니다.

`csv` 파일 양식은 [example.csv](./example.csv) 참고 하세요.

## Requirements

* `python3` (`3.7.4`에서 개발했습니다.)

## How to run

먼저 의존 모듈을 설치합니다:

```
$ pip install -r requirements.txt
```

다음과 같이 실행할 수 있습니다:

```bash
$ python sender.py example.csv
```

실제 사용 예:

```bash
$ python sender.py example.csv
1122333, 332211
총 2 명. 진행합니까? (Y/n): y
 target uid: 8368411
-  image sent
-  message sent
 target uid: 8891844
-  image sent
-  message sent
Done.
```

전송 전에 물어보는 과정이 있으며, 로그가 출력됩니다.

도중에 취소해야 한다면 `ctrl + c` 를 입력하세요.

### 백그라운드로 실행하기

백그라운드로 실행하기 위해선 `nohup` 명령어를 이용합니다.
이 방법은 **전송 전 물어보는 과정이 없으므로 주의**해서 사용해야 합니다.

```bash
$ echo y | nohup python -u sender.py example.csv &
```

실제 사용 예:

```bash
buntalk ❯ echo y | nohup python -u sender.py example.csv &
[1] 11200 11201
appending output to nohup.out
```

이 방법을 사용하면 `nohup.out` 파일이 생성되고, 로그가 기록됩니다.
`tail -F nohup.out` 명령어로 실시간으로 로그를 확인할 수 있습니다.
