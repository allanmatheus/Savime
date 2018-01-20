/*
*    This program is free software: you can redistribute it and/or modify
*    it under the terms of the GNU General Public License as published by
*    the Free Software Foundation, either version 3 of the License, or
*    (at your option) any later version.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU General Public License for more details.
*
*    You should have received a copy of the GNU General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
*    HERMANO L. S. LUSTOSA				JANUARY 2018
*/
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h> 
#include <sys/un.h>
#include <sys/mman.h> 
#include <fstream>
#include <sys/stat.h>
#include <iostream>
#include <iomanip>
#include <list>
#include <algorithm>
#include <../lib/protocol.h>
#include <../lib/savime_lib.h>

#define _BUFFER 2048

void error(const char *msg)
{
    perror(msg);
    exit(0);
}

void setHeader(char * buffer, int total_size){
    MessageHeader header;
    header.magic = 0x42;
    header.protocol_version = 0x01;
    header.payload_length = total_size;
    memcpy(buffer, (char *)&header, sizeof(header));
}

std::ifstream::pos_type filesize(const char* filename)
{
    std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);
    return in.tellg(); 
}


 void print_header(QueryResultHandle handle)
 {
    std::list<std::string> el;
   
    for(auto entry : handle.schema)
    {
        if(entry.second.is_dimension)
                el.push_back(entry.first);    
    }
    
    for(auto entry : handle.schema)
    {
        if(!entry.second.is_dimension)
                el.push_back(entry.first);
    }
     
    std::cout <<  std::string(19*handle.schema.size()+handle.schema.size()+1, '-') << std::endl;

    for(auto entry : el)    
    {
        std::cout << "|" << std::right << std::setw(19) << std::setfill(' ') << entry;
    }
    
    std::cout << "|" << std::endl;
    std::cout <<  std::string(19*handle.schema.size()+handle.schema.size()+1, '-') << std::endl;
 }

 void print_block(QueryResultHandle& handle)
 {
    int64_t entry_count = 0, minimal_count = 0;
    std::map<std::string, char*> buf_map;
    for(auto entry : handle.schema)
    {
        struct stat s; fstat(handle.descriptors[entry.first], &s);
        
        buf_map[entry.first] = (char*)mmap(0, s.st_size, PROT_READ, MAP_SHARED, handle.descriptors[entry.first], 0);
        
        if (buf_map[entry.first] == MAP_FAILED) {
            close(handle.descriptors[entry.first]);
            perror("Error while mapping file");
            return;
        }
       
        switch(entry.second.type)
        {
            case SAV_INTEGER: entry_count = s.st_size/sizeof(int32_t); break;
            case SAV_LONG: entry_count = s.st_size/sizeof(int64_t) ; break;
            case SAV_FLOAT: entry_count = s.st_size/sizeof(float) ; break;
            case SAV_DOUBLE: entry_count = s.st_size/sizeof(double) ; break;
        }
        
        if(minimal_count == 0 || entry_count < minimal_count)
        {
            minimal_count = entry_count;
        }
    }
   
    entry_count =  minimal_count;
    
    std::list<std::string> el;
   
    for(auto entry : handle.schema)
    {
        if(entry.second.is_dimension)
                el.push_back(entry.first);
        
    }
    
    for(auto entry : handle.schema)
    {
        if(!entry.second.is_dimension)
                el.push_back(entry.first);
        
    }
        
    for(int64_t i = 0;  i < entry_count; i++)
    {
        for(auto element : el)
        {
            switch(handle.schema[element].type)
            {
                case SAV_INTEGER:  std::cout << "|" << std::right << std::setw(19) << std::setfill(' ') << ((int32_t*)buf_map[element])[i]; break;
                case SAV_LONG: std::cout << "|" << std::right << std::setw(19) << std::setfill(' ') << ((int64_t*)buf_map[element])[i]; break;
                case SAV_FLOAT: std::cout << "|" << std::right << std::setw(19) << std::setfill(' ') <<  std::fixed << std::showpoint << std::setprecision(4) << ((float*)buf_map[element])[i]; break;
                case SAV_DOUBLE: std::cout << "|" << std::right << std::setw(19) << std::setfill(' ') <<  std::fixed << std::showpoint << std::setprecision(4) << ((double*)buf_map[element])[i]; break;
            }
        }
        
        std::cout << "|" << std::endl;
    }
    
    std::cout <<  std::string(19*handle.schema.size()+handle.schema.size()+1, '-') << std::endl;
    
 }
 
void process_query_response(SavConn& con, QueryResultHandle& handle, bool printResult)
{
    if(handle.is_schema == 0)
    {
        printf("%s\n", handle.response_text);
        read_query_block(con, handle);
    }
    else
    {
        print_header(handle);
        while(true)
        {
            int ret = read_query_block(con, handle);
            if(!ret) break;
            if(ret == -1)
            {
                printf("%s\n", handle.response_text);
                break;
            }
            
            if(printResult)
                print_block(handle);
            
            for(auto entry : handle.descriptors)
            {
                close(entry.second);
            }
            handle.descriptors.clear();
        }
    }      
}

int main(int argc, char *argv[])
{
    char query[_BUFFER];
    //SavConn con = open_connection(65000, "127.0.0.1");
    SavConn con = open_connection(0, "");
    QueryResultHandle handle;
    
    if(argc == 1)
    {
        while(true)
        {
            memset(query, sizeof(char), '\0');
            printf("query> ");
            fgets(query, _BUFFER, stdin);
            handle = execute(con, query);
            process_query_response(con, handle, true);
            dipose_query_handle(handle);
        }
    }
    else
    {
        handle = execute(con, argv[1]);
        
        if(argc == 3)
            process_query_response(con, handle, false);
        else
            process_query_response(con, handle, true);
        
        dipose_query_handle(handle);
    }
    
    close_connection(con);
}