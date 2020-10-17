# Disk-based B+Tree Implementation

## A. Porting to C++
기존의 소스코드는 C를 기반으로 작성돼있지만, C++로 전부 포팅했다. C++ standard로는 c++1z(c++17)를 채택했다.

## B. Binary search vs Linear search
기존의 소스코드에선 key를 검색하는 것을 전부 linear serach로 수행했다. 하지만 node 내부에서 key는 sorted 상태이기 때문에 binary search를 사용하면 performance가 좋아질 것이라 가정하고 실험을 수행했다.

> **insert key**  
> linear search : 3.69386 seconds ( 100,000회 insert, 50회 평균 )  
> binary search : 3.10306 seconds ( 100,000회 insert, 50회 평균 )

> **find key**  
> linear search : 2.70108 seconds ( 100,000회 search, 50회 평균 )  
> binary search : 2.41896 seconds ( 100,000회 search, 50회 평균 )

그 결과 binary search가 linear search보다 insert에선 약 16%, find에선 약 10% 성능 향상을 보였다. 따라서 key값을 찾는 부분을 모두 binary search로 교체했다.

## C. Page API
### a. Page data type
page 관련 크기는 전부 상수로 지정하여 page 내부의 branch 혹은 record의 개수가 그에 따라 정해지도록 했다. 또한 file header / free page / internal page / leaf page를 모두 담을 수 있도록 `page_t`를 위의 naive design과 조금 다르게 설계했다. 변경된 `page_t`는 다음과 같다.

```c++
union page_t
{
    header_page_t file;

    struct
    {
        union
        {
            page_header_t header;
            free_page_header_t free_header;
        };

        union
        {
            page_data_t data[PAGE_DATA_IN_PAGE];
            page_branch_t branch[PAGE_BRANCHES_IN_PAGE];
        };
    } node;
};
```

다만 `page_t`가 복잡해져 코드가 지져분해지는 문제를 해결하고 pagenum을 page data와 같이 관리하기 위해 `Page`란 wrapper class를 만들었다.

### b. Page read/write
Page를 쓰고 읽을 때 file manager를 바로 호출하는 것이 아닌, `Page` 클래스의 `load`, `commit` method를 호출하는 방식으로 되어있기 때문에 buffer manager layer가 추가 되어도 index manager의 별도 수정 없이도 buffer manager를 도입할 수 있을 것이라 기대된다.

## D. Delayed Merge
`BPTree`에 `MERGE_THRESHOLD`란 상수를 두어 delayed merge 수행시 key 개수의 기준을 설정하도록 하였다. key의 개수가 `MERGE_THRESHOLD`가 되면 merge operation이 수행된다.

현재 설정된 `MERGE_THRESHOLD`에선 redistribute operation이 수행되지는 않지만, 이후 설정에 따라 수행될 수도 있어 redistribute operation도 구현하였다. 다만 기존 방식인 무조건 왼쪽/오른쪽 node로의 key 이동이 아닌, key가 적게 있는 node로 이동하도록 구현해 merge operation이 덜 일어나도록 하여 조금 더 효율적으로 동작하게 하였다.

## E. Layered Architecture
### a. File Manager Layer
**file**: file.h, file.cpp, page.h, page.cpp  
File Manager의 동작은 file descriptor를 가지고 있는 singleton pattern의 `FileManager` 클래스에 모두 구현돼있다.

### b. Index Manager Layer
**file**: bpt.h, bpt.cpp  
Index Manager의 동작은 B+Tree의 구현체인 singleton pattern을 채택한 `BPTree` 클래스에 구현돼있다.

**<비고>**  
File Manager와 API Layer간의 상호 작용(파일 열기)을 위해 `open` method를 두었다.

### c. API Layer
**file**: dbapi.h, dbapi.cpp  
API Layer에선 `BPTree`의 `open`, `insert`, `remove`, `find` 등의 method를 호출해 각 명령에 맞는 동작을 수행하도록 하였다.
