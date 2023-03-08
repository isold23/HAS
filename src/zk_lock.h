#ifndef _ZOOKEEPER_LOCK_H_
#define _ZOOKEEPER_LOCK_H_

#include <algorithm>
#include <vector>
#include <fstream>

#include <zookeeper.h>

#include "include.h"
#include "config.h"

class CZKLock
{
public:
    CZKLock() : zk(NULL), zk_host(""), lock_node(""), recv_timeout(9), connected(-1), create_node_state(0), fd(-1), node_value(""), locked(false), connected_index(-1) {}
    ~CZKLock() {}
    
    int init(CConfig& conf, pid_t pid);
    int destory();
    int run();
    
    static void zk_log_callback(const char* message);
    static void watcher(zhandle_t* zzh, int type, int state, const char* path, void* context);
    static void zk_stat_completion(int rc, const struct Stat* stat, const void* data);
    static void zk_aset_stat_completion(int rc, const struct Stat* stat, const void* data);
    static void zk_strings_completion(int rc, const struct String_vector* strings, const void* data);
    static void zk_string_completion(int rc, const char* name, const void* data);
    static char* lookupnode(const struct String_vector* vector, const char* prefix, bool& is_small);
    static void zk_data_completion(int rc, const char* value, int value_len, const struct Stat* stat, const void* data);
    
private:
    int start();
    int stop(int rc);
    int lock(bool is_lock);
    int unlock();
    int trylock();
    bool islocked();
    int timeout();
    std::string get_config();
    int set_config(std::string& config);
    bool try_lock_file(int fd);
    bool unlock_file(int fd);
    
public:
    zhandle_t* zk;
    static int path_exist;
    uint64_t last_timestamp;
private:
    std::string zk_host;
    std::string zk_path;
    std::string lock_node;
    std::string start_command;
    std::string stop_command;
    std::vector<std::string> progress_list;
    //recv timeout. default 9s
    uint32_t recv_timeout;
    //0 connected -1 not connected
    int32_t connected;
    //0 not create 1 creating 2 created
    int32_t create_node_state;
    uint32_t idle_time;
    int fd;
    int interest;
    struct timeval tv;
    std::string node_value;
    bool locked;
    int connected_index;
    std::string host;
    uint32_t update_config_idle_time;
    uint64_t last_update_config;
    uint32_t check_program_idle_time;
    uint64_t last_check_program;
    pid_t mpid;
};

#endif //_ZOOKEEPER_LOCK_H_
