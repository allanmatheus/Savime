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
#ifndef SAVIME_LIB_H
#define SAVIME_LIB_H

#include <map>
#include <netdb.h>

struct SavConn
{
    int socketfd;
    int clientid;
    int queryid;
    int message_count;
    bool is_rdma_enabled;
    char rdma_host[NI_MAXHOST];
    char rdma_service[NI_MAXSERV];
};

enum SavType {SAV_INTEGER, SAV_LONG, SAV_FLOAT, SAV_DOUBLE, INVALID_TYPE};

struct SavDataElement
{
    std::string name;
    int is_dimension;
    SavType type;
};

struct QueryResultHandle
{
    char * response_text;
    int is_schema;
    std::map<std::string, int> descriptors;
    std::map<std::string, std::string> files;
    std::map<std::string, SavDataElement> schema;
};

struct BufferSet
{
    char ** buffers;
    char ** set_name;
    int * buffer_sizes;
    int num_buffers;
};

struct FileBufferSet
{
    char ** files;
    char ** set_name;
    size_t * file_sizes;
    int num_files;
};

void hello_lib();
int has_file_parameters(char * query, FileBufferSet* file_buffer_set);
SavConn open_connection(int port, const char * address);
QueryResultHandle execute(SavConn& connection, char * query);
int read_query_block(SavConn& connection, QueryResultHandle& result_handle);
QueryResultHandle execute(SavConn& connection, char * query, FileBufferSet file_buffer_set);
QueryResultHandle execute(SavConn& connection, char * query, BufferSet buffer_set);
void dipose_query_handle(QueryResultHandle& queryHandle);
void close_connection(SavConn& conn);

#endif /* SAVIME_LIB_H */

