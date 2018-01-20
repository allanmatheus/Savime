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
#ifndef RDMAP_H
#define RDMAP_H

#include <cstddef>

typedef void (*pre_conn_cb_fn) (struct rdma_cm_id * id);
typedef void (*connect_cb_fn) (struct rdma_cm_id * id);
typedef void (*completion_cb_fn) (struct ibv_wc * wc);
typedef void (*disconnect_cb_fn) (struct rdma_cm_id * id);

void rc_init(pre_conn_cb_fn, connect_cb_fn, completion_cb_fn,
        disconnect_cb_fn);

int run_rdma_server(const char *host, int port);

int run_rdma_client(const char *host, int port, int fd, size_t size);

#endif /* RDMAP_H */
