#ifndef COMMON_H_
#define COMMON_H_

inline constexpr int SUCCESS = 0;
inline constexpr int FAIL = 1;

#define SUCCESSED(cond) ((cond) == SUCCESS)
#define FAILED(cond) ((cond) != SUCCESS)

#define CHECK_FAILURE(cond) \
    if (!(cond))            \
    {                       \
        return false;       \
    }
#define CHECK_FAILURE2(cond, val) \
    if (!(cond))                  \
    {                             \
        return val;               \
    }

#endif  // COMMON_H_
