#ifndef RECOVERY_H_
#define RECOVERY_H_

#include "log.h"
#include "types.h"

#include <fstream>
#include <memory>
#include <string>
#include <unordered_map>

enum class RecoveryMode
{
    NORMAL,
    REDO_CRASH,
    UNDO_CRASH
};

class Recovery final
{
 public:
    Recovery(const std::string& logmsg_path,
             RecoveryMode mode, int log_num);
    ~Recovery();

    void start();

 private:
    void analyse();
    [[nodiscard]] bool redo();
    [[nodiscard]] bool undo();

 private:
    RecoveryMode mode_;
    int log_num_;

    std::ofstream f_log_msg_;

    std::unordered_map<lsn_t, Log> logs_;
    std::unordered_map<xact_id, bool> xacts_;
    std::unordered_map<xact_id, lsn_t> losers_;
};

#endif  // RECOVERY_H_
