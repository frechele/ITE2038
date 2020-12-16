#ifndef RECOVERY_H_
#define RECOVERY_H_

#include <fstream>
#include <string>

class Recovery final
{
 public:
    Recovery(const std::string& log, const std::string& logmsg);

 private:
    std::ofstream f_log_;
};

#endif  // RECOVERY_H_
