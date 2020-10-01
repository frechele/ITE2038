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
