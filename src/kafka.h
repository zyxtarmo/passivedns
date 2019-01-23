/*
** This file is a part of PassiveDNS.
** 
** Copyright (C) 2019, Tarmo Randel <tarmo.randel@gmail.com>
** 
** This program is free software; you can redistribute it and/or modify
** it under the terms of the GNU General Public License as published by
** the Free Software Foundation; either version 2 of the License, or
** (at your option) any later version.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
** GNU General Public License for more details.
**
** You should have received a copy of the GNU General Public License
** along with this program; if not, write to the Free Software
** Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
*/

#define BROKER_PATH "/brokers/ids"

int init_kafka();
void shutdown_kafka();
int send_query_data_to_kafka(uint8_t is_err_record, char *kafkadata, size_t len);
void msg_delivered (rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque);
void error_cb (rd_kafka_t *rk, int err, const char *reason, void *opaque);
static void set_brokerlist_from_zookeeper(zhandle_t *zzh, char *brokers);
static zhandle_t* initialize_zookeeper(const char * zookeeper, const int debug);
static void watcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx);