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
#include <list>
#include <fstream>
#include <sys/types.h>
#include <arpa/inet.h>   
#include <sys/socket.h>
#include <sys/sendfile.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netdb.h> 
#include <sys/un.h>
#include <vector>
#include <../rdmap/rdmap.h>
#include <../lib/protocol.h>
#include <../lib/savime_lib.h>

#define __BUFSIZE 4096
#define __PATHSIZE 1024
#define __UNIX_SOCKET_ADDRESS "/dev/shm/savime-socket"
#define __TMP_FILES_PATH "/dev/shm/"

//int sockfd, portno, n;
struct sockaddr_in serv_addr;
struct sockaddr_un serv_addr_un;
struct hostent *server;

std::vector<std::string> split(const std::string& str, const char& ch) 
{
    std::string next; std::vector<std::string> result;
 
    for (std::string::const_iterator it = str.begin(); it != str.end(); it++) 
    {
        if (*it == ch) 
        {
            if (!next.empty()) 
            {
                result.push_back(next);
                next.clear();
            }
        } 
        else 
        {
            next += *it;
        }
    }
    
    if (!next.empty())
         result.push_back(next);
    
    return result;
}


std::ifstream::pos_type get_file_size(const char* filename)
{
    std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);
    return in.tellg(); 
}

//UTIL FUNCTIONS
int savime_connect(const char * unix_socket_address)
{
    int sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sockfd < 0) 
        perror("Error opening socket");

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr_un.sun_family = AF_UNIX;

    strcpy(serv_addr_un.sun_path, unix_socket_address);

    if (connect(sockfd,(struct sockaddr *) &serv_addr_un,sizeof(serv_addr_un)) < 0) 
        perror("Error connecting");
    
    return sockfd;
}

int savime_connect(int portno, const char * address)
{
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) 
        perror("Error opening socket");
    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;

    serv_addr.sin_port = htons(portno);
    serv_addr.sin_addr.s_addr = inet_addr(address);
    
    if (connect(sockfd,(struct sockaddr *) &serv_addr,sizeof(serv_addr)) < 0) 
        perror("Error connecting");
    
    return sockfd;
}

int savime_receive(int socket, char * buffer, size_t size)
{
    off64_t total_received = 0, to_receive = size;
     
    while(to_receive > 0)
    {
        off64_t received = recv(socket, &buffer[total_received], size, 0);

        if(received == 0)
            break;
        
        if(received < 0)
        {
            perror("Error receiving data.");
            exit(0);
        }
                
        total_received += received;
        to_receive -= received;
    }
    
    return 0;
}

int savime_receive(int socket, int file, size_t size)
{ 
    off_t input_offset = 0, output_offset = 0; off_t len;
    size_t total_transferred = 0; int transferred_bits;
    int pipe_descriptors[2]; size_t buffer_size = __BUFSIZE;

    if(pipe(pipe_descriptors) < 0)
    {
        perror("Error creating pipes.");
        exit(0);
    }

    while(total_transferred < size)
    {
        size_t partial_transfer = 0;        
        if((size-total_transferred) < buffer_size) buffer_size = (size-total_transferred);
        
        while(partial_transfer < buffer_size)
        {
            size_t to_transfer = buffer_size - partial_transfer;
            transferred_bits = splice(socket, NULL, pipe_descriptors[1], NULL, to_transfer, SPLICE_F_MOVE | SPLICE_F_MORE);
            if(transferred_bits < 0 )
            {
               perror("Problem during splice operation.");
               exit(0);
            }
            partial_transfer+=transferred_bits;
        }
            
        partial_transfer = 0;
        
        while(partial_transfer < buffer_size)
        {
            size_t to_transfer = buffer_size - partial_transfer;
            //transferred_bits = splice(pipe_descriptors[0], NULL, file, &output_offset, to_transfer, SPLICE_F_MOVE | SPLICE_F_MORE);
            transferred_bits = splice(pipe_descriptors[0], NULL, file, NULL, to_transfer, SPLICE_F_MOVE | SPLICE_F_MORE);
            if(transferred_bits < 0)
            {
               perror("Problem during splice operation.");
               exit(0);
            }
            partial_transfer+=transferred_bits;
        }

        total_transferred += buffer_size;
    }

    close(pipe_descriptors[0]);
    close(pipe_descriptors[1]);
    
    return 0;
}

int savime_read_rdma(SavConn& connection, int fd, size_t size)
{
    return -1;
}

int savime_send(int socket, char * buffer, size_t size)
{
    off64_t total_send = 0, to_send = size;
    
    while(to_send > 0)
    {
        size_t send = write(socket, &buffer[total_send], to_send);     
        
       
        if(send < 0)
        {
            perror("Error while sending data.");
            exit(0);
        }

        total_send += send;
        to_send -= send;
    }
    
    return 0;
}

int savime_send(int socket, int file, size_t size)
{
    ssize_t total_send = 0, to_send = size;
   
    while(to_send > 0)
    {
        ssize_t send = sendfile64(socket, file, (off64_t*)&total_send, to_send);
        printf("send %ld %ld %ld\n", send, total_send, to_send);
        //if(send == 0) for(;;);
        
        if(send < 0)
        {
            perror("Error while sending data from file.");
            exit(0);
        }
        
        //total_send += send;
        to_send -= send;
    }
    
    return 0;
}

int savime_write_rdma(SavConn& connection, int fd, size_t size)
{
    return -1;
}

int savime_wait_ack(int socket)
{
    MessageHeader header;
    savime_receive(socket, (char*)&header, sizeof(MessageHeader));
}

int savime_get_appendable_file(QueryResultHandle& result_handle, char * block_name)
{
    std::string sblock_name(block_name);
    
    if(result_handle.descriptors.find(sblock_name) != result_handle.descriptors.end())
    {
        return result_handle.descriptors[sblock_name];
    }
    
    char path[__PATHSIZE] = "\0";
    strncat(path, __TMP_FILES_PATH, strlen(__TMP_FILES_PATH));
    strncat(path, block_name, strlen(block_name));
    remove(path);
    
    int file = open(path, O_CREAT| O_RDWR, 0666);
    if(file < 0)
    {
        perror(path);
        exit(0);
    }
    
    result_handle.descriptors[sblock_name] = file;
    result_handle.files[sblock_name] = path;
    return file;
}

void send_query(SavConn& connection, char * query)
{
    int query_length = strlen(query);
    
    //Create header
    MessageHeader header;
    init_header(header, connection.clientid, connection.queryid, connection.message_count++, C_CREATE_QUERY_REQUEST, 0, NULL, NULL);
    
    //Send message and wait response
    savime_send(connection.socketfd, (char*)&header, sizeof(MessageHeader));
    savime_receive(connection.socketfd, (char*)&header, sizeof(MessageHeader));
    
    //Storing queryid returned by server
    connection.queryid = header.queryid;
    
    //Send query
    init_header(header, connection.clientid, connection.queryid, connection.message_count++, C_SEND_QUERY_TXT, query_length+1, NULL, NULL);
    savime_send(connection.socketfd, (char*)&header, sizeof(header));
    savime_send(connection.socketfd, query, query_length+1);  
    savime_wait_ack(connection.socketfd);
    
    //Send query done signal
    init_header(header, connection.clientid, connection.queryid, connection.message_count++, C_SEND_QUERY_DONE, 0, NULL, NULL);
    savime_send(connection.socketfd, (char*)&header, sizeof(header));
    savime_wait_ack(connection.socketfd);
}

void send_query_params(SavConn& connection,  FileBufferSet file_buffer_set)
{
    //send param data
    MessageHeader header;

    for(int i = 0; i < file_buffer_set.num_files; i++)
    {
        //Send Param Request
        init_header(header, connection.clientid, connection.queryid, connection.message_count++, C_SEND_PARAM_REQUEST, 0, NULL, NULL);
        savime_send(connection.socketfd, (char*)&header, sizeof(header));
        savime_wait_ack(connection.socketfd);

        //Send Param Data
        init_header_block(header, connection.clientid, connection.queryid, connection.message_count++, C_SEND_PARAM_DATA, file_buffer_set.file_sizes[i], file_buffer_set.set_name[i], 1,  NULL, NULL);
        savime_send(connection.socketfd, (char*)&header, sizeof(header));
        int file = open(file_buffer_set.files[i], O_RDONLY);
        if(file < 0)
        {
            perror("Could not open file");
            exit(0);
        }
        
        if(connection.is_rdma_enabled)
        {
            savime_write_rdma(connection, file, file_buffer_set.file_sizes[i]);
        }
        else
        {
            savime_send(connection.socketfd, file, file_buffer_set.file_sizes[i]);
        }
        
        close(file);
        savime_wait_ack(connection.socketfd);

        //Send param done
        init_header(header, connection.clientid, connection.queryid, connection.message_count++, C_SEND_PARAM_DONE, 0, NULL, NULL);
        savime_send(connection.socketfd, (char*)&header, sizeof(header));
        savime_wait_ack(connection.socketfd);
    } 
}

void send_query_params(SavConn& connection,  BufferSet buffer_set)
{
    //send param data
    MessageHeader header;

    for(int i = 0; i < buffer_set.num_buffers; i++)
    {
        //Send Param Request
        init_header(header, connection.clientid, connection.queryid, connection.message_count++, C_SEND_PARAM_REQUEST, 0, NULL, NULL);
        savime_send(connection.socketfd, (char*)&header, sizeof(header));
        savime_wait_ack(connection.socketfd);

        //Send Param Data
        init_header_block(header, connection.clientid, connection.queryid, connection.message_count++, C_SEND_PARAM_DATA, buffer_set.buffer_sizes[i], buffer_set.set_name[i], 1, NULL, NULL);
        savime_send(connection.socketfd, (char*)&header, sizeof(header));
        savime_send(connection.socketfd, buffer_set.buffers[i], buffer_set.buffer_sizes[i]);
        savime_wait_ack(connection.socketfd);

        //Send param done
        init_header(header, connection.clientid, connection.queryid, connection.message_count++, C_SEND_PARAM_DONE, 0, NULL, NULL);
        savime_send(connection.socketfd, (char*)&header, sizeof(header));
        savime_wait_ack(connection.socketfd);
    } 
}

void send_result_request(SavConn& connection)
{
    //Create header
    MessageHeader header;
    
    //Request result from server
    init_header(header, connection.clientid, connection.queryid, connection.message_count++, C_RESULT_REQUEST, 0, NULL, NULL);
    savime_send(connection.socketfd, (char*)&header, sizeof(header));
    savime_wait_ack(connection.socketfd);
    init_header(header, connection.clientid, connection.queryid, connection.message_count++, C_ACK, 0, NULL, NULL);
    savime_send(connection.socketfd, (char*)&header, sizeof(header));
}

void receive_query(SavConn& connection, QueryResultHandle& result_handle)
{
     //Create header
    MessageHeader header;
    
    //Starting processing query result
    savime_receive(connection.socketfd, (char*)&header, sizeof(MessageHeader));
    result_handle.response_text = (char*) malloc(header.payload_length);
    savime_receive(connection.socketfd, result_handle.response_text, header.payload_length);
    result_handle.is_schema = (header.type == S_SEND_SCHEMA) ? 1 : 0;
    init_header(header, connection.clientid, connection.queryid, connection.message_count++, C_ACK, 0, NULL, NULL);
    savime_send(connection.socketfd, (char*)&header, sizeof(header));
}

void receive_query_data(SavConn& connection, QueryResultHandle& result_handle)
{
    //Create header
    MessageHeader header;
    
    //Reading response
    while(true)
    {
        savime_receive(connection.socketfd, (char*)&header, sizeof(MessageHeader));
        if(header.type == S_RESPONSE_END)
        {
            break;
        }

        if(header.payload_length != 0)
        {
            int file = savime_get_appendable_file(result_handle, header.block_name);
            
            if(connection.is_rdma_enabled)
            {
                savime_read_rdma(connection, file, header.payload_length);
            }
            else
            {
                savime_receive(connection.socketfd, file, header.payload_length);
            }
        }

       //Send ACK
       init_header(header, connection.clientid, connection.queryid, connection.message_count++, C_ACK, 0, NULL, NULL);
       savime_send(connection.socketfd, (char*)&header, sizeof(header));
    } 
}

void receive_response_end(SavConn& connection)
{
    MessageHeader header;
    savime_receive(connection.socketfd, (char*)&header, sizeof(MessageHeader));
}

int has_file_parameters(char * query, FileBufferSet* file_buffer_set)
{
    size_t i=0, position = 0;
    std::string text(query);
    std::list<std::string> files;
    
    while(true)
    {
        size_t start = text.find("\"", position);
        if(start == std::string::npos) break;
        size_t end = text.find("\"", start+1);
        if(end == std::string::npos) break;
        position = end+1;
        
        std::string param = text.substr(start+1, end-start-1);
        if(param.at(0) == '@')
            files.push_back(param);
    }
     
    file_buffer_set->num_files = files.size();
    file_buffer_set->files = (char**) malloc(sizeof(char*)* files.size());
    file_buffer_set->file_sizes = (size_t*) malloc(sizeof(size_t)* files.size());
    file_buffer_set->set_name = (char**) malloc(sizeof(char*)* files.size());
  
    for(auto& f : files)
    {
        file_buffer_set->files[i] = (char*)malloc(sizeof(char)*(f.length()+1));
        strncpy(file_buffer_set->files[i], &f.c_str()[1], f.length());
        file_buffer_set->file_sizes[i] = get_file_size(file_buffer_set->files[i]);
        printf("file:%s %ld\n", file_buffer_set->files[i], file_buffer_set->file_sizes[i]);
        
        std::string param_name = std::string("param")+std::to_string(i);
        file_buffer_set->set_name[i] = (char*)malloc(sizeof(char)*(param_name.length()+1));
        strncpy(file_buffer_set->set_name[i], param_name.c_str(), param_name.length()+1);
        i++;
    }  
    
    return files.size();
}

/*return 0 if there are no blocks left and 1 otherwise. Returns -1 on error*/
int read_query_block(SavConn& connection, QueryResultHandle& result_handle)
{
    //Create header
    MessageHeader header;

    for(auto fileEntry : result_handle.files)
    {
        if(remove(fileEntry.second.c_str()) == -1)
            perror("Could not remove file");
    }
    result_handle.descriptors.clear();
    result_handle.files.clear();
    
    for(int i = 0; i < result_handle.schema.size(); i++)
    {    
        
        savime_receive(connection.socketfd, (char*)&header, sizeof(MessageHeader));
        
        if(header.type == S_RESPONSE_END)
        {
            return 0;
        }
         
        if(header.type == S_SEND_TEXT)
        {
            result_handle.response_text = (char*) malloc(header.payload_length);
            savime_receive(connection.socketfd, result_handle.response_text, header.payload_length);
            init_header(header, connection.clientid, connection.queryid, connection.message_count++, C_ACK, 0, NULL, NULL);
            savime_send(connection.socketfd, (char*)&header, sizeof(header));
            return -1;
        }   
        else
        {    
            if(header.payload_length != 0)
            {
                int file = savime_get_appendable_file(result_handle, header.block_name);

                if(connection.is_rdma_enabled)
                {
                    savime_read_rdma(connection, file, header.payload_length);
                }
                else
                {
                    savime_receive(connection.socketfd, file, header.payload_length);
                }
            }
            
            //Send ACK
            init_header(header, connection.clientid, connection.queryid, connection.message_count++, C_ACK, 0, NULL, NULL);
            savime_send(connection.socketfd, (char*)&header, sizeof(header));
        }
    }
    
    return 1;
}

int parse_schema(QueryResultHandle& result_handle)
{
    if(result_handle.response_text[0] == SCHEMA_IDENTIFIER_CHAR)
    {
        result_handle.is_schema = 1;
        std::string schema_string(&result_handle.response_text[1]);
        std::vector<std::string> schema_elements = split(schema_string, '|');
        
        for(auto schema_element : schema_elements)
        {
            SavDataElement dataElement;
            std::vector<std::string> schema_detais = split(schema_element, ',');
            dataElement.name = schema_detais[0];
            dataElement.is_dimension = schema_detais[1].c_str()[0] == 'd' ? 1 : 0;
            
            switch(schema_detais[2].c_str()[0])
            {
                case 'i' : dataElement.type = SAV_INTEGER; break;
                case 'l' : dataElement.type = SAV_LONG; break;
                case 'f' : dataElement.type = SAV_FLOAT; break;
                case 'd' : dataElement.type = SAV_DOUBLE; break;
            }
            
            result_handle.schema[dataElement.name] = dataElement;
        }
    }
    else
    {
        result_handle.is_schema = 0;
    }
    
    return 1;
}

void dispose_file_buffer_set(FileBufferSet* file_buffer_set)
{
    for(int i = 0; i < file_buffer_set->num_files; i++)
    {
        free(file_buffer_set->files[i]);
        free(file_buffer_set->set_name[i]);
    }
    
    free(file_buffer_set->files);
    free(file_buffer_set->file_sizes);
    free(file_buffer_set->set_name);
}

//LIB FUNCTIONS
SavConn open_connection(int port, const char * address)
{
    SavConn connection; MessageHeader header;
    connection.message_count = 0;
    connection.is_rdma_enabled = false;        
    memset(connection.rdma_host, 0, NI_MAXHOST);
    memset(connection.rdma_service, 0, NI_MAXSERV);
    
    if(address[0] == '\0')
    {
        connection.socketfd = savime_connect(__UNIX_SOCKET_ADDRESS);
    }
    else
    {
        connection.socketfd = savime_connect(port, address);
    }
    
    init_header(header, 0, 0, connection.message_count++, C_CONNECTION_REQUEST, 0, NULL, NULL);
    savime_send(connection.socketfd, (char*)&header, sizeof(MessageHeader));
    savime_receive(connection.socketfd, (char*)&header, sizeof(MessageHeader));
    
    connection.clientid = header.clientid;
    memcpy(connection.rdma_host, header.rdma_host, NI_MAXHOST);
    memcpy(connection.rdma_service, header.rdma_service, NI_MAXSERV);
    
    return connection;
}

QueryResultHandle execute(SavConn& connection, char * query)
{
    MessageHeader header; FileBufferSet file_buffer_set;
    QueryResultHandle result_handle;
   
    //Send query
    //printf("---Sending query: %s\n", query);
    send_query(connection, query);
    
    if(has_file_parameters(query, &file_buffer_set))
    {
        //printf("---Sending params\n");
        send_query_params(connection, file_buffer_set);
    }
    
    dispose_file_buffer_set(&file_buffer_set);
    
    //printf("---Sending result request\n");
    //Send result request
    send_result_request(connection);

    //printf("---Receive query response\n");
    //Send query response
    receive_query(connection, result_handle);
    
    //printf("---Parsing schema\n");
    //Parsing schema from text response
    parse_schema(result_handle);
    
    
    //if there is no more data, read end response
    if(result_handle.is_schema == 0)
    {
        //printf("---Receive end of response\n");
        receive_response_end(connection);
    }
        
    return result_handle;
}


QueryResultHandle execute(SavConn& connection, char * query, FileBufferSet file_buffer_set)
{
    MessageHeader header;
    QueryResultHandle result_handle;
   
    //Send query
    send_query(connection, query);
    
     //Send query params
    send_query_params(connection, file_buffer_set);
    
    //Send result request
    send_result_request(connection);
   
    //Send query response
    receive_query(connection, result_handle);
   
    //if there is no more data, read end response
    if(result_handle.is_schema == 0)
    {
        receive_response_end(connection);
    }
        
    return result_handle;
}

QueryResultHandle execute(SavConn& connection, char * query, BufferSet buffer_set)
{
    MessageHeader header;
    QueryResultHandle result_handle;
   
    //Send query
    send_query(connection, query);
    
     //Send query params
    send_query_params(connection, buffer_set);
    
    //Send result request
    send_result_request(connection);
   
    //Send query response
    receive_query(connection, result_handle);
   
    //if there is no more data, read end response
    if(result_handle.is_schema == 0)
    {
        receive_response_end(connection);
    }
        
    return result_handle;
}

void dipose_query_handle(QueryResultHandle& queryHandle)
{
    for(auto entry : queryHandle.descriptors)
        close(entry.second);
    
    for(auto entry : queryHandle.files)
        remove(entry.second.c_str());
}

void close_connection(SavConn& connection)
{
    MessageHeader header;
    init_header(header, connection.clientid, connection.queryid, connection.message_count++, C_CLOSE_CONNECTION, 0, NULL, NULL);
    savime_send(connection.socketfd, (char*)&header, sizeof(header));
    close(connection.socketfd);
}
