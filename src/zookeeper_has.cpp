#include <stdio.h>
#include <iostream>
#include <string>
#include <time.h>
#include <signal.h>

#include <sys/types.h>
#include <unistd.h>

#include "include.h"
#include "zk_lock.h"

const std::string gstrProgramVersion = "1.0.0.0";
const std::string gstrProgramName = "zookeeper_has";

CDebugTrace* goDebugTrace = NULL;
bool g_is_running = true;

struct stru_argv {
    std::string config_name;
    std::string log_path;
};

void help_info(void)
{
    std::cout << "High Availability Server(HAS) Help Infomation:" << std::endl;
    std::cout << "./zookeeper_has -f configure_file_name(run application)" << std::endl;
    std::cout << "./zookeeper_has -v(display server version info)" << std::endl;
    std::cout << "./zookeeper_has -h(display help infomation)" << std::endl;
    std::cout << "warning: please use kill -15 pid when stop zookeeper_has" << std::endl;
}

void si_handler(int ai_sig)
{
    if(SIGPIPE == ai_sig) {
        std::cout << "pid: " << getpid() << " SIGPIPE !" << std::endl;
    } else if(SIGTERM == ai_sig) {
        std::cout << "pid: " << getpid() << " SIGTERM !" << std::endl;
        g_is_running = false;
    }
}

int main(int argc, char* argv[])
{
    pid_t pid = getpid();
    stru_argv arg;
    
    if(argc <= 1) {
        help_info();
        return 0;
    }
    
    for(int i = 1; i < argc; i = i + 2) {
        if(strcmp(argv[i], "-v") == 0) {
            std::cout << "High Availability Server(HAS) Version Infomation:" << std::endl;
            std::cout << "HAS Version: " << gstrProgramVersion.c_str() << " " << __DATE__ << " " << __TIME__ << std::endl;
            return 0;
        } else if(strcmp(argv[i], "-h") == 0) {
            help_info();
            return 0;
        } else if(strcmp(argv[i], "-f") == 0) {
            arg.config_name = argv[i + 1];
            std::cout << arg.config_name << std::endl;
            
            if(arg.config_name.empty()) {
                help_info();
                return 0;
            }
        } else if(strcmp(argv[i], "-p") == 0) {
            arg.log_path = argv[i + 1];
            std::cout << arg.log_path << std::endl;
            
            if(arg.log_path.empty()) {
                help_info();
                return 0;
            }
        } else if(strcmp(argv[i], "-l") == 0) {
            if(argc < i + 1) {
                std::cout << "argc error." << std::endl;
                exit(0);
            }
            
            std::string log_properties = argv[i + 1];
            
            if(log_properties.empty()) {
                help_info();
                return 0;
            } else {
                Logger().config(log_properties.c_str());
            }
        }
    }
    
    //goDebugTrace = new CDebugTrace;
    //uint32_t process_id = CCommon::GetProcessId();
    char lszLogFileName[255];
    memset(lszLogFileName, 0, 255);
    
    if(arg.log_path.empty()) {
        CCommon::GetAppPath(lszLogFileName, 255);
        std::string lstrAppPath = lszLogFileName;
        std::string lstrApp = lstrAppPath.substr(0, lstrAppPath.find_last_of('/'));
        
        if(chdir(lstrApp.c_str())) {
            std::cout << gstrProgramName << " error: chdir error." << lszLogFileName << std::endl;
            return 0;
        }
        
        strcpy(strrchr(lszLogFileName, '/'), "//log");
        CCommon::CreatePath(lszLogFileName);
    } else {
        memcpy(lszLogFileName, arg.log_path.c_str(), arg.log_path.length());
    }
    
    /*
    char lszFileDate[50];
    memset(lszFileDate, 0, 50);
    sprintf(lszFileDate, "//%s-%u", gstrProgramName.c_str(), process_id);
    strcat(lszLogFileName, lszFileDate);
    SET_LOG_FILENAME(lszLogFileName);
    SET_TRACE_LEVEL(5);
    TRACE(3, "\n\n*******************" << gstrProgramName << " version:" <<
          gstrProgramVersion.c_str() << "*******************");
    TRACE(3, "configure file name : " << arg.config_name.c_str());
    */
    struct sigaction sig;
    memset(&sig, 0, sizeof(struct sigaction));
    sig.sa_handler = si_handler;
    int ret = sigemptyset(&sig.sa_mask);
    
    if(ret != 0) {
        LOG_FATAL("sigemptyset error. errno: %d", errno);
        return 0;
    }
    
    ret = sigaction(SIGPIPE | SIGTERM, &sig, NULL);
    
    if(ret != 0) {
        LOG_FATAL("set sigle error. errno: %d", errno);
        return 0;
    }
    
    CConfig loConfig;
    loConfig.set_conf_file_name(arg.config_name.c_str());
    bool bRet = loConfig.load();
    
    if(!bRet) {
        LOG_FATAL("load configure file failed.");
        return 0;
    }
    
    CZKLock lock;
    ret = lock.init(loConfig, pid);
    
    if(ret != 0) {
        LOG_FATAL("ZK Lock object init failed.");
        return 0;
    }
    
    while(g_is_running) {
        LOG_DEBUG("------------------------------------");
        int ret = lock.run();
        
        if(ret != 0) {
            LOG_FATAL("ZK Lock run failed.");
            break;
        }
    }
    
    ret = lock.destory();
    LOG_NOTICE("zk lock server exit.");
    return 0;
}
