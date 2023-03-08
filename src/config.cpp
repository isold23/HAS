/**
 * Author: isold.wang@gmail.com
 */
#include "config.h"

bool CConfig::parse_value(const char* key, const char* value)
{
    if(!strcmp(key, "zk_host")) {
        zk_host = value;
        return true;
    }
   
    if(!strcmp(key, "host")) {
        host = value;
        return true;
    }
    
    if(!strcmp(key, "recv_timeout")) {
        recv_timeout = atoi(value);
        return true;
    }
    
    if(!strcmp(key, "update_config_idle_time")) {
        update_config_idle_time = atoi(value);
        return true;
    }
    
    if(!strcmp(key, "check_program_idle_time")) {
        check_program_idle_time = atoi(value);
        return true;
    }
    
    if(!strcmp(key, "zk_path")) {
        zk_path = value;
        return true;
    }
    
    if(!strcmp(key, "start_command")) {
        start_command = value;
        return true;
    }
    
    if(!strcmp(key, "stop_command")) {
        stop_command = value;
        return true;
    }
    
    if(!strcmp(key, "idle_time")) {
        idle_time = atoi(value);
        return true;
    }
    
    if(!strcmp(key, "program_config_file")) {
        std::string program_config_file  = value;
        int ret = parse_array(program_config_file.c_str(), progress_list);
        
        if(ret != 0) {
            return false;
        }
        
        return true;
    }
    
    return true;
}

void CConfig::print()
{
}
