# WARNING: THIS CODE DOES NOT WORK CORRECTLY!!!

# Crash and Recovery

## A. Log File Format
### a. Log Record Type
Log는 다음과 같은 클래스로 표현한다.

```c++
struct Log {
    int size_;
    lsn_t lsn_;
    lsn_t last_lsn_;
    xact_id xid_;
    LogType type_;

    table_id_t tid_;
    pagenum_t pid_;
    int offset_;
    int length_;
    char old_data_[PAGE_DATA_VALUE_SIZE];
    char new_data_[PAGE_DATA_VALUE_SIZE];
    lsn_t next_undo_lsn_;
};
```
padding은 없도록 하였으며, 이후 파일에 저장할 때는 각 log의 type에 따른 크기만큼만 저장하는 방식으로 동작한다. 또한, LSN은 해당 log record의 시작 offset을 의미한다.

### b. Log File
Log file은 다음 형태의 header를 가진다.

```c++
struct log_file_header
{
    lsn_t base_lsn;
    lsn_t next_lsn;
};
```

- base_lsn : log file에 있는 record 중 lsn이 가장 작은 record의 lsn
- next_lsn : 다음 log의 lsn을 나타낸다. 즉, 현재 존재하는 record의 `lsn + size`를 의미한다.

## B. Three Phase Recovery (ARIES)
ARIES protocol에 따른 recovery manager는 `recovery.h`, `recovery.cpp`에 정의된 `Recovery` 클래스로 구현했다. recovery 작업은 각 phase가 다음 a, b, c 순서대로 수행된다.

### a. Analysis Phase
Winner transaction과 Loser transaction을 구분하는 단계이다. 이 단계에선 또한 loser transaction들의 마지막 log sequence number를 구한다.

알고리즘은 다음과 같다.

```
for log in logfile:
    if log.type is BEGIN:
        losers[log.xid] := nil
    elif log.type is in { COMMIT, ROLLBACK }:
        losers -= log.xid
    elif log.type is in { UPDATE, COMPENSATE }:
        losers[log.xid] := log.lsn
```

위 알고리즘대로 수행하면 losers는 key가 transaction id, value는 the largest lsn of trx인 dictionary가 된다.

### b. Redo Phase
DBMS가 crash된 직전의 state로 되돌리는 단계이다. 이 단계에선 analysis phase와 마찬가지로 전체 log file을 순회한다. 또한 page의 lsn과 log의 lsn을 이용하여 consider-redo를 도입해 효과적으로 redo를 수행한다.

### c. Undo Phase
Loser transaction에 대해 stable storage의 db file에 잘못 적힌 부분을 되돌리는 단계이다. 위에서 구한 losers를 이용하여 largest lsn을 가진 log부터 차례대로 undo를 수행한다.

## C. Recovery Manager and Log Manager
Log record를 file에서 읽고 관리하는 작업을 `Recovery`에서 하는 것은 code duplication을 야기할 수 있으며, 이후에 log file의 구조를 바꾸면 Log Manager와 Recovery Manager를 동시에 바꿔야 하는 문제가 생길 수 있다. 그렇기에 Log와 관련된 모든 작업은 `LogManager` 클래스가 담당한다. `Recovery` 클래스는 `LogManager`의 method를 호출하는 방식으로 log에 접근하여 recovery 작업을 수행한다.

## D. Simple Test
자체적으로 진행한 테스트는 다음과 같다.

### a. updates with single thread
single thread에서 여러 transaction을 이용해 `db_update` api를 호출 후 `trx_commit` 혹은 `trx_abort` 없이 dbms 강제 shutdown
결과) 정상적으로 commit된 record에 대해 recovery가 수행된 것을 확인함.

### b. updates with multi thread
5 thread에서 무작위로 `db_update`를 수행하고, 도중 임의의 시점에서 dbms를 강제 shutdown
결과) LSN 발급 등의 작업이 정상적인 것을 확인함.
