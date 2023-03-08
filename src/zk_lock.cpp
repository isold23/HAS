/**
 * Author: isold.wang@gmail.com
 */
#include <sys/select.h>
#include "utility.h"
#include "zk_lock.h"
#include "time_base.h"

static const char* error2str(int error)
{
    switch(error) {
    case ZOK:
        /*!< Everything is OK */
        return "ZOK";
        
    case ZSYSTEMERROR:
        /** System and server-side errors.
        ** This is never thrown by the server, it shouldn't be used other than
        ** to indicate a range. Specifically error codes greater than this
        ** value, but lesser than {@link #ZAPIERROR}, are system errors. */
        return "ZSYSTEMERROR";
        
    case ZRUNTIMEINCONSISTENCY:
        /*!< A runtime inconsistency was found */
        return "ZRUNTIMEINCONSISTENCY";
        
    case ZDATAINCONSISTENCY:
        /*!< A data inconsistency was found */
        return "ZDATAINCONSISTENCY";
        
    case ZCONNECTIONLOSS:
        /*!< Connection to the server has been lost */
        return "ZCONNECTIONLOSS";
        
    case ZMARSHALLINGERROR:
        /*!< Error while marshalling or unmarshalling data */
        return "ZMARSHALLINGERROR";
        
    case ZUNIMPLEMENTED:
        /*!< Operation is unimplemented */
        return "ZUNIMPLEMENTED";
        
    case ZOPERATIONTIMEOUT:
        /*!< Operation timeout */
        return "ZOPERATIONTIMEOUT";
        
    case ZBADARGUMENTS:
        /*!< Invalid arguments */
        return "ZBADARGUMENTS";
        
    case ZINVALIDSTATE:
        /*!< Invliad zhandle state */
        return "ZINVALIDSTATE";
        
    //case ZNEWCONFIGNOQUORUM:
    /*!< No quorum of new config is connected and
     *up-to-date with the leader of last commmitted
     *config - try invoking reconfiguration after new
     *servers are connected and synced */
    //return "ZNEWCONFIGNOQUORUM";
    //case ZRECONFIGINPROGRESS:
    /*!< Reconfiguration requested while another
     *reconfiguration is currently in progress. This
     *is currently not supported. Please retry. */
    //return "ZRECONFIGINPROGRESS";
    case ZAPIERROR:
        return "ZAPIERROR";
        
    case ZNONODE:
        return "ZNONODE";
        
    case ZNOAUTH:
        return "ZNOAUTH";
        
    case ZBADVERSION:
        return "ZBADVERSION";
        
    case ZNOCHILDRENFOREPHEMERALS:
        return "ZNOCHILDRENFOREPHEMERALS";
        
    case ZNODEEXISTS:
        return "ZNODEEXISTS";
        
    case ZNOTEMPTY:
        return "ZNOTEMPTY";
        
    case ZSESSIONEXPIRED:
        return "ZSESSIONEXPIRED";
        
    case ZINVALIDCALLBACK:
        return "ZINVALIDCALLBACK";
        
    case ZINVALIDACL:
        return "ZINVALIDACL";
        
    case ZAUTHFAILED:
        return "ZAUTHFAILED";
        
    case ZCLOSING:
        return "ZCLOSING";
        
    case ZNOTHING:
        return "ZNOTHING";
        
    case ZSESSIONMOVED:
        return "ZSESSIONMOVED";
        //case ZNOTREADONLY:
        //return "ZNOTREADONLY";
        //case ZEPHEMERALONLOCALSESSION:
        //return "ZEPHEMERALONLOCALSESSION";
        //case ZNOWATCHER:
        //return "ZNOWATCHER";
        //case ZRWSERVERFOUND:
        //return "ZRWSERVERFOUND";
    }
    
    return "UNKNOW_ERROR";
}

static const char* state2str(int state)
{
    if(state == 0)
        return "CLOSED_STATE";
        
    if(state == ZOO_CONNECTING_STATE)
        return "CONNECTING_STATE";
        
    if(state == ZOO_ASSOCIATING_STATE)
        return "ASSOCIATING_STATE";
        
    if(state == ZOO_CONNECTED_STATE)
        return "CONNECTED_STATE";
        
    /*
    if(state == ZOO_READONLY_STATE)
        return "READONLY_STATE";
    */
    
    if(state == ZOO_EXPIRED_SESSION_STATE)
        return "EXPIRED_SESSION_STATE";
        
    if(state == ZOO_AUTH_FAILED_STATE)
        return "AUTH_FAILED_STATE";
        
    return "INVALID_STATE";
}

static const char* type2str(int type)
{
    if(type == ZOO_CREATED_EVENT)
        return "CREATED_EVENT";
        
    if(type == ZOO_DELETED_EVENT)
        return "DELETED_EVENT";
        
    if(type == ZOO_CHANGED_EVENT)
        return "CHANGED_EVENT";
        
    if(type == ZOO_CHILD_EVENT)
        return "CHILD_EVENT";
        
    if(type == ZOO_SESSION_EVENT)
        return "SESSION_EVENT";
        
    if(type == ZOO_NOTWATCHING_EVENT)
        return "NOTWATCHING_EVENT";
        
    return "UNKNOWN_EVENT_TYPE";
}

int CZKLock::path_exist = 0;

char* CZKLock::lookupnode(const struct String_vector* vector, const char* prefix, bool& is_small)
{
    char* ret = NULL;
    
    if(strlen(prefix) == 0) {
        return ret;
    }
    
    if(vector->data) {
        int i = 0;
        std::vector<std::string> task_list;
        
        for(i = 0; i < vector->count; i++) {
            char* child = vector->data[i];
            LOG_DEBUG("vector string: %s, prefix: %s", child, prefix);
            std::string task_name = child;
            task_list.push_back(task_name);
            
            if(strncmp(prefix, child, strlen(prefix)) == 0) {
                ret = strdup(child);
            }
        }
        
        if(!task_list.empty()) {
            std::sort(task_list.begin(), task_list.end());
            
            if(ret != NULL) {
                if(strncmp(ret, task_list[0].c_str(), strlen(ret)) == 0) {
                    is_small = true;
                }
            }
        }
    }
    
    return ret;
}

void CZKLock::watcher(zhandle_t* zzh, int type, int state, const char* path, void* context)
{
    LOG_DEBUG("watcher type: %s, state: %s", type2str(type), state2str(state));
    CZKLock* lock = reinterpret_cast<CZKLock*>(context);
    
    if(lock == NULL) {
        LOG_DEBUG("lock is null. watcher type: %s, state: %s", type2str(type), state2str(state));
        return;
    }
    
    if(type == ZOO_SESSION_EVENT) {
        if(state == ZOO_CONNECTED_STATE) {
            LOG_NOTICE("connect succ.");
            lock->connected = 0;
            int ret = zoo_aexists(lock->zk, lock->zk_path.c_str(), 1, zk_stat_completion, lock);
            
            if(ret != ZOK) {
                LOG_WARNING("zookeeper_aexists error. ret: %s", error2str(ret));
                return;
            }
            
            //lock->connected_index = zoo_get_connect_index(lock->zk);
            //TRACE(5, "connected index : " << lock->connected_index);
        } else if(state == ZOO_CONNECTING_STATE) {
            LOG_NOTICE("current state : %s", state2str(state));
        } else {
            LOG_DEBUG("session event state: %s", state2str(state));
            lock->stop(state);
        }
    } else if(type == ZOO_CHANGED_EVENT) {
        LOG_DEBUG("path: %s, context: %s, lock host: %s", path, context, lock->host.c_str());
        int ret = zoo_aget(lock->zk, lock->zk_path.c_str(), 1, zk_data_completion, lock);
        
        if(ret != ZOK) {
            LOG_WARNING("zoo_aget error. ret: %s", error2str(ret));
        }
    } else if(type == ZOO_CHILD_EVENT) {
        if(!lock->locked) {
            int ret = zoo_aget_children(lock->zk, lock->zk_path.c_str(), 1, zk_strings_completion, lock);
            
            if(ret != ZOK) {
                LOG_WARNING("zoo_aget_chidren error. ret: %s", error2str(ret));
                return;
            }
        }
    } else {
        LOG_DEBUG("type: %s", type2str(type));
    }
}

void CZKLock::zk_stat_completion(int rc, const struct Stat* stat, const void* data)
{
    const CZKLock* lock = reinterpret_cast<const CZKLock*>(data);
    
    if(lock == NULL) {
        LOG_NOTICE("lock is null.");
        return;
    }
    
    CZKLock::path_exist = -1;
    
    if(rc == ZOK) {
        CZKLock::path_exist = 1;
        int ret = zoo_aget(lock->zk, lock->zk_path.c_str(), 1, zk_data_completion, lock);
        
        if(ret != ZOK) {
            LOG_WARNING("zoo_aget error. ret: %s", error2str(ret));
        }
    } else if(rc == ZNONODE) {
        LOG_WARNING("path: %s not exist.", lock->zk_path.c_str());
        exit(0);
    } else {
        LOG_DEBUG("zk_stat_completion data: %s, rc: %s", lock->zk_path.c_str(), error2str(rc));
    }
}

void CZKLock::zk_aset_stat_completion(int rc, const struct Stat* stat, const void* data)
{
    if(rc != ZOK) {
        LOG_DEBUG("aset error: %s", error2str(rc));
    }
}

void CZKLock::zk_string_completion(int rc, const char* name, const void* data)
{
    CZKLock* lock = (CZKLock*)(data);
    
    if(lock == NULL) {
        LOG_FATAL("zk_string_completion lock is null.");
        return;
    }
    
    if(rc != ZOK) {
        LOG_FATAL("zk_string_completion error. rc: %s", error2str(rc));
        lock->create_node_state = 0;
        return;
    }
    
    std::string strName = name;
    size_t pos = strName.find_last_of("/");
    std::string ss = strName.substr(pos + 1, strName.length() - pos);
    lock->lock_node = ss;
    lock->create_node_state = 2;
    LOG_DEBUG("zk_string_completion name: %s", ss.c_str());
    int ret = zoo_aget_children(lock->zk, lock->zk_path.c_str(), 1, zk_strings_completion, lock);
    
    if(ret != ZOK) {
        LOG_WARNING("zoo_aget_chidren error. ret: %s", error2str(ret));
        return;
    }
}

void CZKLock::zk_strings_completion(int rc, const struct String_vector* strings, const void* data)
{
    if(rc != ZOK) {
        LOG_FATAL("zk_strings_completion error. rc: %s", error2str(rc));
        return;
    }
    
    CZKLock* lock = (CZKLock*)(data);
    
    if(lock == NULL) {
        LOG_FATAL("zk_strings_completion lock is null.");
        deallocate_String_vector((String_vector*)strings);
        return;
    }
    
    char prefix[30];
    const clientid_t* cid = zoo_client_id(lock->zk);
    // get the session id
    int64_t session = cid->client_id;
    snprintf(prefix, 30, "x-%016lx-", session);
    bool is_small = false;
    char* id = lookupnode(strings, lock->lock_node.c_str(), is_small);
    
    if(id == NULL) {
        if(lock->create_node_state == 0) {
            std::string path = lock->zk_path + "/lock";
            rc = zoo_acreate(lock->zk, path.c_str(), lock->host.c_str(), lock->host.length(),
                             &ZOO_OPEN_ACL_UNSAFE,  ZOO_SEQUENCE | ZOO_EPHEMERAL,
                             zk_string_completion, lock);
                             
            if(rc != ZOK) {
                LOG_WARNING("create lock error. rc: %d", rc);
            } else {
                lock->create_node_state = 1;
            }
        }
    } else {
        int ret = lock->lock(is_small);
        
        if(ret != 0) {
            LOG_WARNING("lock error.");
        }
        
        free(id);
        id = NULL;
    }
}

void CZKLock::zk_data_completion(int rc, const char* value, int value_len,
                                 const struct Stat* stat, const void* data)
{
    CZKLock* lock = (CZKLock*)(data);
    
    if(lock == NULL) {
        LOG_FATAL("zk_data_completion lock is null.");
        return;
    }
    
    if(rc != ZOK) {
        LOG_FATAL("zk_data_completion error. rc: %s", error2str(rc));
        return;
    }
    
    bool first = lock->node_value.empty() ? true : false;
    char buffer[1024 * 10];
    memset(buffer, 0, sizeof(buffer));
    memcpy(buffer, value, value_len);
    lock->node_value = buffer;
    
    if((lock->locked && first) || (!lock->locked)) {
        int ret = lock->set_config(lock->node_value);
        
        if(ret != 0) {
            LOG_WARNING("set config error.");
            return;
        }
    LOG_NOTICE("set zookeeper node value: %s, value len: %d", lock->node_value.c_str(), value_len);
    }
    
    if(lock->create_node_state == 0) {
        std::string path = lock->zk_path + "/lock";
        LOG_NOTICE("create node : %s", lock->host.c_str());
        rc = zoo_acreate(lock->zk, path.c_str(), lock->host.c_str(), lock->host.length(),
                         &ZOO_OPEN_ACL_UNSAFE,  ZOO_SEQUENCE | ZOO_EPHEMERAL,
                         zk_string_completion, lock);
                         
        if(rc != ZOK) {
            LOG_WARNING("create lock error. rc: %d", rc);
        } else {
            lock->create_node_state = 1;
        }
    }
}

void CZKLock::zk_log_callback(const char* message)
{
    LOG_DEBUG("zk log: %s", message);
}

int CZKLock::init(CConfig& conf, pid_t pid)
{
    mpid = pid;
    zk_host = conf.zk_host;
    recv_timeout = conf.recv_timeout;
    zk_path = conf.zk_path;
    idle_time = conf.idle_time;
    start_command = conf.start_command;
    stop_command = conf.stop_command;
    progress_list = conf.progress_list;
    host = GetLocalIpStr();
    update_config_idle_time = conf.update_config_idle_time;
    check_program_idle_time = conf.check_program_idle_time;
    zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
    last_timestamp = CTimeBase::get_current_time();
    last_update_config = CTimeBase::get_current_time();
    last_check_program = CTimeBase::get_current_time();
	if(host.empty()) {
		return -1;
	}
    return 0;
}

int CZKLock::destory()
{
    LOG_DEBUG("desctory");
    int rc = 0;
    stop(rc);
    
    if(zk != NULL) {
        zookeeper_close(zk);
        zk = NULL;
    }
    
    return 0;
}

int CZKLock::start()
{
    if(zk == NULL) {
        zk = zookeeper_init(zk_host.c_str(), watcher, recv_timeout, 0, this, 0);
        
        if(zk == NULL) {
            LOG_NOTICE("zk init failed.");
            return -1;
        }
        
        LOG_DEBUG("zk init successful.");
        //zoo_set_log_callback(zk, zk_log_callback);
    }
    
    int state = zoo_state(zk);
    
    if(state != ZOO_CONNECTED_STATE) {
        LOG_NOTICE("state : %s", state2str(state));
    } else {
        LOG_DEBUG("state : %s", state2str(state));
    }
    
    LOG_DEBUG("fd : %d, insterest: %d, tv.sec: %d, tv.usec: %d", fd, interest, tv.tv_sec, tv.tv_usec);
    //zoo_set_log_stream(NULL);
    int ret = zookeeper_interest(zk, &fd, &interest, &tv);
    LOG_DEBUG("fd : %d, insterest: %d, tv.sec: %d, tv.usec: %d", fd, interest, tv.tv_sec, tv.tv_usec);
    state = zoo_state(zk);
    
    if(state != ZOO_CONNECTED_STATE) {
        LOG_NOTICE("state : %s", state2str(state));
    } else {
        LOG_DEBUG("state : %s", state2str(state));
    }
    
    if(ret != ZOK) {
        LOG_WARNING("zookeeper_interest error. ret: %s", error2str(ret));
        return -1;
    }
    
    last_timestamp = CTimeBase::get_current_time();
    return 0;
}

int CZKLock::stop(int rc)
{
    LOG_NOTICE("lock stop");
    locked = false;
    CZKLock::path_exist = 0;
    node_value = "";
    lock_node = "";
    create_node_state = 0;
    unlock();
    last_timestamp = CTimeBase::get_current_time();
    
    if(zk != NULL) {
        zookeeper_close(zk);
        zk = NULL;
    }
    
    //TRACE(5, "last timestamp: " << last_timestamp);
    return 0;
}

int CZKLock::timeout()
{
    uint64_t now = CTimeBase::get_current_time();
    
    if(now - last_timestamp > idle_time * 1000 * 1000) {
        LOG_DEBUG("timeout. now: %lu, last timestamp: %lu, idle time: %lu", now, last_timestamp, idle_time);
        return -1;
    }
    
    return 0;
}

int CZKLock::run()
{
    fd_set rfds, wfds, efds;
    int ret = start();
    
    if(ret != 0) {
        LOG_WARNING("start error.");
        stop(-1);
        return 0;
    }
    
    int state = zoo_state(zk);
    //const int current_connect_index = zoo_get_connect_index(zk);
    //TRACE(5, "current_connect_index: " << current_connect_index << " connected index: " << connected_index);
    
    if(state != ZOO_CONNECTED_STATE) {
        LOG_NOTICE("state : %s", state2str(state));
    }

    struct sockaddr addr;
    
    socklen_t addr_len = sizeof(addr);
    
    sockaddr* paddr = zookeeper_get_connected_host(zk, &addr, &addr_len);
    
    if(paddr != NULL) {
    }
    
    if(fd == -1) {
        LOG_NOTICE("socket is invalid.");
        stop(-1);
        return 0;
    }
    
    int recv_timeout = zoo_recv_timeout(zk);
    LOG_DEBUG(" zoo_recv_timeout recv timeout : %lu", recv_timeout);
    FD_ZERO(&rfds);
    FD_ZERO(&wfds);
    FD_ZERO(&efds);
    
    if(interest & ZOOKEEPER_READ) {
        FD_SET(fd, &rfds);
    }
    
    if(interest & ZOOKEEPER_WRITE) {
        FD_SET(fd, &wfds);
    }
    
    FD_SET(fd, &efds);
    int events = 0;
    LOG_DEBUG("select sec: %lu, usec: %lu", tv.tv_sec, tv.tv_usec);
    //struct timeval select_timeout;
    //select_timeout.tv_sec = 2;
    //select_timeout.tv_usec = 0;
    //TRACE(5, "select sec: " << select_timeout.tv_sec << " usec: " << select_timeout.tv_usec);
    ret = select(fd + 1, &rfds, &wfds, &efds, &tv);
    
    if(ret > 0) {
        if(FD_ISSET(fd, &rfds)) {
            LOG_DEBUG("Read event.");
            events |= ZOOKEEPER_READ;
            last_timestamp = CTimeBase::get_current_time();
        }
        
        if(FD_ISSET(fd, &wfds)) {
            LOG_DEBUG("Write event.");
            events |= ZOOKEEPER_WRITE;
        }
        
        if(FD_ISSET(fd, &efds)) {
            LOG_DEBUG("Exception event.");
        }
    } else if(ret == 0) {
        LOG_NOTICE("Select timeout ret = 0");
    } else if(ret < 0) {
        LOG_WARNING("select ret < 0, errno: %d", errno);
        events = 0;
    }
    
    if(zk == NULL) return 0;
    
    if(events != 0) {
        ret = zookeeper_process(zk, events);
        
        if(ret != ZOK && ret != ZNOTHING) {
            LOG_WARNING("zookeeper process error. ret: %s", error2str(ret));
            stop(ret);
            return 0;
        }
    }
    
    //if(zk == NULL) return 0;
    ret = timeout();
    
    if(ret != 0) {
        LOG_WARNING("timeout. stop");
        stop(ret);
        return 0;
    }
    
    state = zoo_state(zk);
    
    if(state != ZOO_CONNECTED_STATE) {
        LOG_NOTICE("state : %s", state2str(state));
        return 0;
    }
    
    uint64_t now = CTimeBase::get_current_time();
    
    if(locked && (now - last_check_program > check_program_idle_time * 1000)) {
        int ret = lock(locked);
        
        if(ret != 0) {
            LOG_WARNING("check program error.");
        }
        
        last_check_program = now;
    }
    
    if(locked && (now - last_update_config > update_config_idle_time * 1000)) {
        int ret = lock(locked);
        
        if(ret != 0) {
            LOG_WARNING("check program error.");
        }
        
        std::string value = get_config();
        
        if(!value.empty()) {
            int ret = zoo_aset(zk, zk_path.c_str(), value.c_str(), value.length(), -1, zk_aset_stat_completion, this);
            
            if(ret != ZOK) {
                LOG_WARNING("zoo_aset error. ret: %s", error2str(ret));
            }
        }
        
        last_update_config = now;
        LOG_NOTICE("update zookeeper node value: %s", value.c_str());
    }
    
    return 0;
}

std::string CZKLock::get_config()
{
    uint64_t timestamp = CTimeBase::get_current_time();
    char sz_now[50];
    memset(sz_now, 0, 50);
    sprintf(sz_now, "%lu", timestamp);
    std::string now = sz_now;
    std::string ret = "last_time=" + now + ";" + "program_config={";
    std::vector<std::string>::iterator iter = progress_list.begin();
    bool is_error = false;
    
    for(; iter != progress_list.end(); ++iter) {
        std::string file_name = *iter;
        
        if(file_name != "NULL") {
            int fd = open(file_name.c_str(), O_APPEND | O_CREAT | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
            
            if(fd == -1) {
                LOG_FATAL("open file error. file name: %s", file_name.c_str());
				is_error = true;
            } else {
                if(try_lock_file(fd)) {
                    char buffer[1024 * 10];
                    memset(buffer, 0, sizeof(buffer));
                    size_t length = sizeof(buffer);
                    int r_ret = read(fd, buffer, length);
                    
                    if(r_ret > 0 && r_ret < int(sizeof(buffer))) {
                        std::string str(buffer);
                        str = str.substr(0, str.length());
                        ret += str;
                    } else {
                        LOG_FATAL("file empty or overflow. file name: %s", file_name.c_str());
                        is_error = true;
                    }
                    
                    if(!unlock_file(fd)) {
                        LOG_FATAL("unlock file failed. file name: %s", file_name.c_str());
						is_error = true;
                    }
                }
                
                close(fd);
            }
        }
        
        ret += ",";
    }
    
    ret = ret.substr(0, ret.length() - 1);
    ret += "}";
    
    if(is_error) {
        ret = "";
    }
    
    LOG_DEBUG("get config: %s", ret.c_str());
    return ret;
}

int CZKLock::set_config(std::string& config)
{
    size_t start_pos = config.find_first_of("{");
    size_t end_pos = config.find_first_of("}");
    if(start_pos == std::string::npos || end_pos == std::string::npos) {
        LOG_ERROR("dont found { or }. config info: %s", config.c_str());
        return -1;
    }
    std::string info = config.substr((start_pos + 1), (end_pos - start_pos - 1));
    LOG_DEBUG("set_config info: %s, config: %s", info.c_str(), config.c_str());
    std::vector<std::string>::iterator iter = progress_list.begin();
    size_t value_start_pos = 0;
    size_t value_end_pos = 0;
    
    for(; iter != progress_list.end(); ++iter) {
        value_end_pos = info.find_first_of(",", value_start_pos);
        
        if(value_end_pos == std::string::npos) {
            value_end_pos = info.length();
        }
        
        std::string value = info.substr(value_start_pos, value_end_pos - value_start_pos);
        LOG_DEBUG("set config: %s", value.c_str());
        
        if(value != "NULL") {
            value_start_pos = value_end_pos + 1;
            std::string file_name = *iter;
            std::ofstream out(file_name.c_str());
            
            if(out.good()) {
                out << value;
            }
            
            out.close();
        }
    }
    
    return 0;
}

int CZKLock::lock(bool is_lock)
{
    locked = is_lock;
    
    if(is_lock) {
        LOG_DEBUG("get lock. start command: %s", start_command.c_str());
        char spid[100];
        memset(spid, 0, 100);
        snprintf(spid, 100, "%d", mpid);
        std::string command = "/bin/sh " + start_command + " " + spid;
        int ret = system(command.c_str());
        if(ret != 0) {
            LOG_WARNING("system ret : %d", ret);
        }
    } else {
        LOG_DEBUG("not get lock.");
    }
    
    return 0;
}

int CZKLock::unlock()
{
    std::string command = "/bin/sh " + stop_command;
    int ret = system(command.c_str());
    if(ret != 0) {
        LOG_WARNING("system ret : %d", ret);
    }
    return 0;
}

int CZKLock::trylock()
{
    return 0;
}

bool CZKLock::islocked()
{
    return locked;
}

bool CZKLock::try_lock_file(int fd)
{
    struct flock fl;
    bzero(&fl, sizeof(struct flock));
    fl.l_pid = getpid();
    fl.l_type = F_WRLCK;
    fl.l_whence = SEEK_END;
    
    if(fcntl(fd, F_SETLKW, &fl) == -1) {
        LOG_FATAL("lock failed. errno: %d", errno);
        return false;
    }
    
    return true;
}

bool CZKLock::unlock_file(int fd)
{
    struct flock fl;
    bzero(&fl, sizeof(struct flock));
    fl.l_pid = getpid();
    fl.l_type = F_UNLCK;
    fl.l_whence = SEEK_END;
    
    if(fcntl(fd, F_SETLK, &fl) == -1) {
        LOG_FATAL("unlock failed. errno: %d", errno);
        return false;
    }
    
    return true;
}
