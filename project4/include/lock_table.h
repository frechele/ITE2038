#ifndef __LOCK_TABLE_H__
#define __LOCK_TABLE_H__

#include <stdint.h>

#define CHECK_FAILURE(cond) if (!(cond)) { return false; }
#define CHECK_FAILURE2(cond, ret_val) if (!(cond)) { return (ret_val); }

typedef struct lock_t lock_t;

/* APIs for lock table */
int init_lock_table();
lock_t* lock_acquire(int table_id, int64_t key);
int lock_release(lock_t* lock_obj);

#endif /* __LOCK_TABLE_H__ */
