
#ifndef _CONFIG_H_
#define _CONFIG_H_

#include <base/utility/logger.h>
#include <base/utility/message.h>
using Biz::Logger;
using Biz::message;

#include "include.h"
#include "configure.h"

inline void traceLevel(int level, int line, std::string function, const char* format, ...)
{
    char buf[1024 * 1024];
    va_list argptr;
    va_start(argptr, format);
    vsnprintf(buf, 1024 * 1024, format, argptr);
    switch(level) {
        case 6:
            Logger().trace("trace", message(function) << ":" << line << " " << buf);
            break; 
        case 5:
            Logger().debug("debug", message(function) << ":" << line << " " << buf);
            break; 
        case 4:
            Logger().info("info", message(function) << ":" << line << " " << buf);
            break;
        case 3:
            Logger().error("error", message(function) << ":" << line << " " << buf);
            break;
        case 1:
            Logger().fatal("fatal", message(function) << ":" << line << " " << buf);
            break;
        default:
            break;
    }
    va_end(argptr);
}
 
#define LOG_TRACE(_fmt_, ...) traceLevel(6,__LINE__, __FILE__, _fmt_, ##__VA_ARGS__)
#define LOG_DEBUG(_fmt_, ...) traceLevel(5,__LINE__, __FILE__, _fmt_, ##__VA_ARGS__)
#define LOG_NOTICE(_fmt_, ...) traceLevel(4,__LINE__, __FILE__, _fmt_, ##__VA_ARGS__)
#define LOG_WARNING(_fmt_, ...) traceLevel(3,__LINE__, __FILE__, _fmt_, ##__VA_ARGS__)
#define LOG_ERROR(_fmt_, ...) traceLevel(2,__LINE__, __FILE__, _fmt_, ##__VA_ARGS__)
#define LOG_FATAL(_fmt_, ...) traceLevel(1,__LINE__, __FILE__, _fmt_, ##__VA_ARGS__)

class CConfig : public CConfigure
{
public:
    CConfig() {}
    virtual ~CConfig() {}
    
    virtual bool parse_value(const char* key, const char* value);
    virtual void print(void);
    
public:
    std::string zk_host;
    uint32_t recv_timeout;
    std::string zk_path;
    std::string start_command;
    std::string stop_command;
    std::vector<std::string> progress_list;
    //s
    uint32_t idle_time;
    std::string host;
    uint32_t update_config_idle_time;
    uint32_t check_program_idle_time;
};

#endif//_CONFIG_H_

