# Disk-based B+Tree

## 1. Analysis of bpt source code
### A. Data types
#### a. record
``` c++
typedef struct record {
    int value;
} record;
```
bpt에 저장된 data를 나타내는 구조체다. 또한 tree를 이루는 가장 작은 데이터 단위이다.  
- **Field**
  - value - record의 값

#### b. node
``` c++
typedef struct node {
    void ** pointers;
    int * keys;
    struct node * parent;
    bool is_leaf;
    int num_keys;
    struct node * next;
} node;
```
bpt에서 node를 나타내는 구조체다.
- **Field**
  - pointers
    - internal node일 경우  
    자식 node의 배열
    - leaf node일 경우  
    record의 배열 (단, 마지막 element는 다음 leaf node의 주소를 담음)
  - keys : node에 있는 key의 배열
  - parent : 부모 node의 주소 (만약 root node라면 NULL)
  - is_leaf : leaf node인지 여부
  - num_keys : 유효한 key의 개수
  - next : queue 자료구조를 위한 field

### B. Insert operation
insert operation은 bpt.c의 `insert` 함수에서 수행된다. `insert` 함수의 prototype은 다음과 같다.
``` c++
node* insert(node* root, int key, int value)
```
- **Return**
  - insert operation 수행 이후 tree의 root node
- **Argument**
  - root : insert operation을 수행할 tree의 root node (단, NULL은 빈 tree를 의미)
  - key : insert될 value와 쌍을 이루는 key
  - value : insert할 value

insert operation은 총 4가지의 case가 존재하며, 각 case 별로 insert operation의 동작 방식이 달라진다.

##### # Case 1 : duplicated key
우선 `insert` 함수는 `find` 함수를 호출하여 key가 duplicated 되는지를 확인한다. 만약 동일한 key를 가진 node가 발견된다면 입력받은 root을 반환하며 insert operation을 종료한다.

단, Case 1이 아닌 경우, `make_record` 함수를 호출해 value를 갖는 새로운 record를 생성한다.

##### # Case 2 : tree does not exists
본 case는 인자로 넘어온 root이 NULL일 때이다. `start_new_tree` 함수를 호출하고, 그 반환 값을 그대로 반환한다.  
`start_new_tree` 함수는 `make_leaf` 함수를 호출하는데, `make_leaf` 함수는 `make_node` 함수를 호출해 node를 생성하고, `is_leaf` field에 true를 assign 한다. 그 이후에 `start_new_tree` 함수에서 key와 record 등을 assign 한다.

##### # Case 3 : tree already exists
tree가 존재하는 경우엔 `find_leaf` 함수를 호출해 leaf를 찾는다. `find_leaf` 함수는 root에서 leaf까지 `is_leaf` field를 체크하며 해당하는 key에 맞는 leaf를 찾는다.

##### # Case 3-1 : leaf is not full
leaf에 새로운 key를 넣을 수 있다면 (leaf가 가지고 있는 key의 개수가 (order - 1)보다 적으면) `insert_into_leaf` 함수를 호출한다. 그 이후 인자로 받은 root을 반환한 뒤 함수를 빠져나간다.

##### # Case 3-2 : leaf is full
leaf가 가득 찬 상태라면 leaf를 split 한 뒤 insert 해야 한다. 따라서 이 경우엔 `insert_into_leaf_after_splitting` 함수를 호출하고, 그 반환 값을 그대로 반환한다.

### C. Delete operation
delete operation은 bpt.c의 `delete` 함수에서 수행된다. `delete` 함수의 prototype은 다음과 같다.
``` c++
node* delete(node* root, int key)
```
- **Return**
  - delete operation 수행 이후 tree의 root node
- **Argument**
  - root : delete operation을 수행할 tree의 root node
  - key : delete할 key

우선 `find` 함수를 호출해 주어진 key에 해당하는 record를 찾는다. 그리고 `find_leaf` 함수를 호출해 key에 해당하는 leaf를 찾는다. 만약 key에 해당하는 record나 leaf가 하나라도 없다면 아무런 동작도 하지 않고 인자로 받은 root을 바로 반환하며 함수를 빠져나간다. 그렇지 않은 경우엔 `delete_entry` 함수를 호출한다.

`delete_entry` 함수는 제일 처음 `remove_entry_from_node` 함수를 호출해 key를 node에서 삭제하고 적절하게 key를 이동시킨다. 그 후 총 4가지 case에 대해 다른 동작을 수행한다.

##### # Case 1 : deletion in root node
root node에서 delete를 하는 경우 `adjust_root` 함수를 호출해 root을 변경한다. 만약 tree가 empty일 경우 NULL을 반환한다.

##### # Case 2 : node stays at or above minimum
`cut` 함수를 호출해 key의 최소 개수를 결정하고 node에 key가 최소 조건 이상인 경우 root 자체를 반환한다.

##### # Case 3 : node falls below minimum
위에서 key의 최소 조건을 만족하지 않는 경우 merge나 redistribution을 수행해야 한다. 우선 `get_neighbor_index` 함수를 호출해 neighbor node의 위치를 찾는다.

##### # Case 3-1 : can merge with neighbor node
neighbor node에 key를 담을 공간이 충분하면 neighbor node와 merge를 수행한다. 이때 `coalesce_nodes` 함수를 호출하고, 그 반환 값을 바로 반환한다.

##### # Case 3-2 : cannot merge with neighbor node
neighbor node에 key를 담을 공간이 부족하면 redistribute를 수행한다. 이때 `redistribute_nodes` 함수를 호출하고, 그 반환 값을 바로 반환한다.

### D. Split operation
split operation은 `insert_into_leaf_after_splitting`에서 시작한다. 이 함수는 우선 `make_leaf` 함수를 호출해 새로운 leaf를 만든다. 그 이후 key에 맞는 insertion point를 찾은 뒤 record를 임시 key-value 공간에 넣는다. 그 후, `cut` 함수를 통해 split point를 찾고 원래의 leaf를 split한다. 그 이후 `insert_into_parent` 함수를 호출해 split operation 과정에서 새로 생긴 node를 tree에 insert 한다.

`insert_into_parent` 함수는 3가지 case에 대해 다른 동작을 수행한다.

##### # Case 1 : new root
parent가 NULL인 상황일 때 `insert_into_new_root` 함수를 호출하고, 그 반환 값을 바로 반환한다. `insert_into_new_root` 함수는 `make_node` 함수를 호출해 root node를 만들고 그 아래에 left subtree, right subtree를 넣는다.

##### # Case 2 : leaf or node
우선 `get_left_index` 함수를 호출해 parent node에서 left node에 대한 index를 찾는다.

##### # Case 2-1 : node is not full
parent node에 새로운 key를 insert할 수 있다면 `insert_indo_node` 함수를 호출하고, 그 반환 값을 바로 반환한다.

##### # Case 2-2 : node is full
parent node가 가득 찼다면 `insert_into_node_after_splitting` 함수를 호출하고 이후 다시 `insert_into_node_after_splitting` 함수에서 `insert_into_parent` 함수를 호출 하는 방식으로 작동한다.

### E. Merge operation
merge operation은 `coalesce_nodes` 함수에서 수행된다. 이 함수에서는 left node에 k_prime과 right node의 key를 넣는다. 그런 뒤, `delete_entry` 함수를 호출해 parent node의 key 중에서 k_prime을 삭제한다.

### F. Redistribute operation
redistribute operation은 `redistribute_nodes` 함수에서 수행된다. redistribute operation은 2가지 case에 대해 다른 동작을 수행한다.

##### # Case 1 : node has a neighbor to the left
node가 leftmost가 아닌 경우 key와 pointer를 우측으로 한 칸씩 이동시키고 0번 칸에는 left node의 마지막 칸의 값을 넣는다. 그 후 그에 맞게 parent node에 대한 pointer와 parent node의 key를 수정한다.

##### # Case 2 : node is leftmost
right node의 0번 칸의 값을 node의 마지막 칸에 넣고, 그에 맞게 parent node에 대한 pointer와 parent node의 key를 수정한다. 그 후 right node에선 key와 pointer를 좌측으로 한 칸씩 이동시킨다.

## 2. Naïve design disk-based B+Tree

### A. Data types
leaf page일 경우 key(8) + value(120)의 record를 담아야하고, internal page일 경우 key(8) + page num(8)의 branch를 담아야 한다. 따라서 크기에 관한 상수와 각각의 자료형을 다음과 같이 만들어준다.

``` c++
constexpr size_t PAGE_DATA_VALUE_SIZE = 120;

struct page_data_t {
    int64_t key;
    char     value[PAGE_DATA_VALUE_SIZE];
};

struct page_branch_t {
    int64_t key;
    pagenum_t child_page_offset;
};
```

이제 각 page를 나타낼 자료형이 필요하다. 각 page는 header와 그 외의 부분으로 나눠진다. 따라서 공통되는 header 크기에 관한 상수와 header 자료형을 다음과 같이 정의한다.

``` c++
constexpr size_t PAGE_HEADER_SIZE = 128;
constexpr size_t PAGE_HEADER_USED = 16;
constexpr size_t PAGE_HEADER_RESERVED = PAGE_HEADER_SIZE - PAGE_HEADER_USED;

struct page_header_t {
    pagenum_t next_free_page_offset;

    int       is_leaf;
    int       num_keys;

    char      reserved[PAGE_HEADER_RESERVED];
};
```
이제 page의 자료형을 만들어야 하는데, internal page와 leaf page를 동시에 담기 위해 다음과 같이 page를 정의한다.

``` c++
constexpr size_t PAGE_SIZE = 4096;
constexpr size_t PAGE_DATA_IN_PAGE = 31;
constexpr size_t PAGE_BRANCHES_IN_PAGE = 248;

struct page_t {
    page_header_t header;

    union {
        page_data_t   data[PAGE_DATA_IN_PAGE];
        page_branch_t branch[PAGE_BRANCHES_IN_PAGE];
    };
};
```

header page관련 상수와 자료형은 별도의 자료형으로 아래와 같이 만든다.

``` c++
constexpr size_t HEADER_PAGE_USED = 24;
constexpr size_t HEADER_PAGE_RESERVED = PAGE_SIZE - HEADER_PAGE_USED;

struct header_page_t {
    uint64_t free_page_number;
    uint64_t root_page_number;
    uint64_t num_pages;

    char     reserved[HEADER_PAGE_RESERVED];
};
```
### B. B+Tree modification

#### a. record
기존의 bpt 코드에서는 데이터를 `record`로 표현했다. 하지만 on-disk B+Tree에서는 `page_data_t`로 표현하므로 `make_record` 함수를 다음과 같이 변경해야한다.

``` c++
page_data_t* make_record(int64_t key, char* value)
```

#### b. node
기존의 bpt의 `node`에 대응되는 것은 `page_t`이다. 따라서 bpt 코드에서 `node`를 모두 `page_t`로 교체해야한다. 또한 index의 타입을 `pagenum_t`로 전부 교체해야한다.

## 3. Implementation

### A. Porting to C++
기존의 소스코드는 C를 기반으로 작성돼있지만, C++로 전부 포팅했다. C++ standard로는 c++1z(c++17)를 채택했다.

### B. Binary search vs Linear search
기존의 소스코드에선 key를 검색하는 것을 전부 linear serach로 수행했다. 하지만 node 내부에서 key는 sorted 상태이기 때문에 binary search를 사용하면 performance가 좋아질 것이라 가정하고 실험을 수행했다.

> **insert key**  
> linear search : 3.69386 seconds ( 100,000회 insert, 50회 평균 )  
> binary search : 3.10306 seconds ( 100,000회 insert, 50회 평균 )

> **find key**  
> linear search : 2.70108 seconds ( 100,000회 search, 50회 평균 )  
> binary search : 2.41896 seconds ( 100,000회 search, 50회 평균 )

그 결과 binary search가 linear search보다 insert에선 약 16%, find에선 약 10% 성능 향상을 보였다. 따라서 key값을 찾는 부분을 모두 binary search로 교체했다.

### C. Page API
#### a. Page data type
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

#### b. Page read/write
Page를 쓰고 읽을 때 file manager를 바로 호출하는 것이 아닌, `Page` 클래스의 `load`, `commit` method를 호출하는 방식으로 되어있기 때문에 buffer manager layer가 추가 되어도 index manager의 별도 수정 없이도 buffer manager를 도입할 수 있을 것이라 기대된다.

### D. Delayed Merge
`BPTree`에 `MERGE_THRESHOLD`란 상수를 두어 delayed merge 수행시 key 개수의 기준을 설정하도록 하였다. key의 개수가 `MERGE_THRESHOLD`가 되면 merge operation이 수행된다.

현재 설정된 `MERGE_THRESHOLD`에선 redistribute operation이 수행되지는 않지만, 이후 설정에 따라 수행될 수도 있어 redistribute operation도 구현하였다. 다만 기존 방식인 무조건 왼쪽/오른쪽 node로의 key 이동이 아닌, key가 적게 있는 node로 이동하도록 구현해 merge operation이 덜 일어나도록 하여 조금 더 효율적으로 동작하게 하였다.

### E. Layered Architecture
#### a. File Manager Layer
**file**: file.h, file.cc, page.h, page.cc  
File Manager의 동작은 file descriptor를 가지고 있는 singleton pattern의 `FileManager` 클래스에 모두 구현돼있다.

#### b. Index Manager Layer
**file**: bpt.h, bpt.cc  
Index Manager의 동작은 B+Tree의 구현체인 singleton pattern을 채택한 `BPTree` 클래스에 구현돼있다.

**<비고>**  
File Manager와 API Layer간의 상호 작용(파일 열기)을 위해 `open` method를 두었다.

#### c. API Layer
**file**: dbapi.h, dbapi.cc  
API Layer에선 `BPTree`의 `open`, `insert`, `remove`, `find` 등의 method를 호출해 각 명령에 맞는 동작을 수행하도록 하였다.