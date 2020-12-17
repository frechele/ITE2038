#include "recovery.h"

#include <fcntl.h>
#include <unistd.h>
#include <cassert>

Recovery::Recovery(const std::string& logmsg_path,
                   RecoveryMode mode, int log_num)
    : mode_(mode), log_num_(log_num)
{
    f_log_msg_.open(logmsg_path);
    assert(f_log_msg_.is_open());
}

Recovery::~Recovery()
{
    f_log_msg_.close();
}

void Recovery::start()
{
    analyse();
}

void Recovery::analyse()
{
    f_log_msg_ << "[ANALYSIS] Analysis pass start\n";

    const lsn_t next_lsn = LogMgr().next_lsn();
    for (lsn_t lsn = LogMgr().base_lsn(); lsn < next_lsn;)
    {
        Log log = LogMgr().read_log(lsn);

        if (log.type() == LogType::BEGIN)
            xacts_[log.xid()] = false;
        else if (log.type() == LogType::COMMIT || log.type() == LogType::ROLLBACK)
            xacts_[log.xid()] = true;

        lsn += log.size();
    }

    f_log_msg_ << "[ANALYSIS] Analysis success. Winner:";
    for (const auto& pr : xacts_)
        if (pr.second)
            f_log_msg_ << ' ' << pr.first;

    f_log_msg_ << ", Loser:";
    for (const auto& pr : xacts_)
        if (!pr.second)
            f_log_msg_ << ' ' << pr.first;

    f_log_msg_ << std::endl;
}
