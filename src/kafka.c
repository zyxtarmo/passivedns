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
**
** Send DNS query response and NXDOMAIN feed to the kafka cluster. Uses the 
** Kafka API from librdkafka (https://github.com/edenhill/librdkafka).
*/

#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <librdkafka/rdkafka.h>
#include <syslog.h>

#include <sys/types.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <pcap.h>
#include "passivedns.h"
#include "kafka.h"

#include <jansson.h>

globalconfig config;

rd_kafka_conf_t *rk_conf;
rd_kafka_topic_conf_t *rkt_conf;
rd_kafka_topic_conf_t *rkt_nxd_conf;

/**
 * Message delivery report callback using the richer rd_kafka_message_t object.
 */
void msg_delivered (rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) 
{
    if (rkmessage->err) {
		selog(LOG_ERR, "Kafka delivery failed: %s", rd_kafka_err2str(rkmessage->err));
		elog("[!] Message delivery failed: %s\n", rd_kafka_err2str(rkmessage->err));
	}
}

void error_cb (rd_kafka_t *rk, int err, const char *reason, void *opaque) 
{
	selog(LOG_ERR, "Kafka error: %s: %s", rd_kafka_err2str(err), reason ? reason : "");
	elog("[!] %s: %s\n", rd_kafka_err2str(err), reason ? reason : "");
}

/**
 * Produce message
 */ 
int send_query_data_to_kafka(uint8_t is_err_record, char *kafkadata, size_t len)
{
	retry:
		if (rd_kafka_produce(
            (is_err_record ? config.rkt_nx : config.rkt_q),	/* Topic object */
            RD_KAFKA_PARTITION_UA,  						/* Use builtin partitioner to select partition*/
            RD_KAFKA_MSG_F_COPY,    						/* Make a copy of the payload. */
            kafkadata, len, NULL, 0, NULL					/* Message payload (value) and length, Optional key and its length */
            ) == -1) {
                selog(LOG_ERR, "Failed to produce to topic %s: %s\n",	/* Failed to *enqueue* message for producing */
					rd_kafka_topic_name((is_err_record ? config.rkt_nx : config.rkt_q)),
                    rd_kafka_err2str(rd_kafka_last_error()));
				
				if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
					rd_kafka_poll(config.rk, 1000); 	/* Max wait 1000 msec */
                    goto retry;
                }
			} else {
				rd_kafka_poll(config.rk, 0); 				/* Non-blocking */
			}
}

/**
 * From https://github.com/edenhill/librdkafka/blob/master/examples/rdkafka_zookeeper_example.c
 */

static void watcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx)
{
	char brokers[1024];
	if (type == ZOO_CHILD_EVENT && strncmp(path, BROKER_PATH, sizeof(BROKER_PATH) - 1) == 0)
	{
		brokers[0] = '\0';
		set_brokerlist_from_zookeeper(zh, brokers);
		if (brokers[0] != '\0' && config.rk != NULL)
		{
			rd_kafka_brokers_add(config.rk, brokers);
			rd_kafka_poll(config.rk, 10);
		}
	}
}

static zhandle_t* initialize_zookeeper(const char * zookeeper, const int debug)
{
	zhandle_t *zh;
	if (debug)
	{
		zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
	}
	zh = zookeeper_init(zookeeper, watcher, 10000, 0, 0, 0);
	if (zh == NULL)
	{
		fprintf(stderr, "Zookeeper connection not established.");
		exit(1);
	}
	return zh;
}

static void set_brokerlist_from_zookeeper(zhandle_t *zzh, char *brokers)
{
	if (zzh)
	{
		struct String_vector brokerlist;
		if (zoo_get_children(zzh, BROKER_PATH, 1, &brokerlist) != ZOK)
		{
			fprintf(stderr, "No brokers found on path %s\n", BROKER_PATH);
			return;
		}

		int i;
		char *brokerptr = brokers;
		for (i = 0; i < brokerlist.count; i++)
		{
			char path[255], cfg[1024];
			sprintf(path, "/brokers/ids/%s", brokerlist.data[i]);
			int len = sizeof(cfg);
			zoo_get(zzh, path, 0, cfg, &len, NULL);

			if (len > 0)
			{
				cfg[len] = '\0';
				json_error_t jerror;
				json_t *jobj = json_loads(cfg, 0, &jerror);
				if (jobj)
				{
					json_t *jhost = json_object_get(jobj, "host");
					json_t *jport = json_object_get(jobj, "port");

					if (jhost && jport)
					{
						const char *host = json_string_value(jhost);
						const int   port = json_integer_value(jport);
						sprintf(brokerptr, "%s:%d", host, port);

						brokerptr += strlen(brokerptr);
						if (i < brokerlist.count - 1)
						{
							*brokerptr++ = ',';
						}
					}
					json_decref(jobj);
				}
			}
		}
		deallocate_String_vector(&brokerlist);
		printf("Found brokers %s\n", brokers);
	}
}

void shutdown_kafka() 
{
	rd_kafka_flush(config.rk, 5000); 					/* Max wait 5 sec */
	rd_kafka_topic_destroy(config.rkt_q);
	rd_kafka_topic_destroy(config.rkt_nx);
    rd_kafka_destroy(config.rk);
    rd_kafka_wait_destroyed(2000);
    zookeeper_close(config.zh);
    selog(LOG_NOTICE, "Kafka connection(s) closed.");
}

/* Initialize Kafka connection, return 0 if success
 */
int init_kafka() {

    char errstr[512];       /* librdkafka API error reporting buffer */
	memset(config.brokers, 0, sizeof(config.brokers));
	int kafka_conf_result = 0;

	rk_conf 		= rd_kafka_conf_new();
	rkt_conf 		= rd_kafka_topic_conf_new();
	rkt_nxd_conf 	= rd_kafka_topic_conf_new();
	
	config.zh = initialize_zookeeper(config.zookeeper, (config.cflags & CONFIG_VERBOSE));
	set_brokerlist_from_zookeeper(config.zh, config.brokers);

	// create kafka conf oject
	if (config.output_kafka_broker == 2) {
		kafka_conf_result = rd_kafka_conf_set(rk_conf, 
				"metadata.broker.list",
				config.brokers, 
				errstr, 
				sizeof(errstr));

	} else {
		kafka_conf_result = rd_kafka_conf_set (rk_conf, 
				"bootstrap.servers", 
				config.kafka_broker, 
				errstr, 
				sizeof(errstr));
	}

	if ( kafka_conf_result != RD_KAFKA_CONF_OK) {
        selog(LOG_ERR, "Kafka conf set error: %s", errstr);
        elog("[!] %s\n", errstr);
        return 1;
    }
    
	// set up delivery report callback
	rd_kafka_conf_set_dr_msg_cb(rk_conf, msg_delivered);
	// set up error callback
	rd_kafka_conf_set_error_cb(rk_conf, error_cb);
	
    // create producer instance
    if (!(config.rk = rd_kafka_new (RD_KAFKA_PRODUCER, rk_conf, errstr, sizeof(errstr)))) {

        selog (LOG_ERR, "Error while kafka_new: %s", errstr);
        elog ("[!] Failed to create new producer: %s\n", errstr);
        return 1;
	}

	if (!(config.rkt_q = rd_kafka_topic_new(config.rk, config.output_kafka_topic, rkt_conf))) {

		selog(LOG_ERR, "Error creating kafka querylog topic: %s\n",
			   rd_kafka_err2str(rd_kafka_last_error()));
		elog("[!] Failed to create querylog topic object: %s\n",
			   rd_kafka_err2str(rd_kafka_last_error()));
		return 1;

    } else {

		selog(LOG_INFO, "Kafka querylog topic created: %s", config.output_kafka_topic);
		olog("Querylog topic object created: %s\n", config.output_kafka_topic);
		config.kafka_broker_ready = 1;
	}

	if (!(config.rkt_nx = rd_kafka_topic_new(config.rk, config.output_kafka_topic, rkt_nxd_conf))) {

		selog(LOG_ERR, "Error creating kafka nxlog topic: %s\n",
			   rd_kafka_err2str(rd_kafka_last_error()));
		elog("[!] Failed to create topic object: %s\n",
				rd_kafka_err2str(rd_kafka_last_error()));
		return 1;

    } else {

		selog(LOG_INFO, "Kafka nxlog topic created: %s", config.output_kafka_topic);
		olog("Topic object created: %s\n", config.output_kafka_topic);
		config.kafka_broker_ready = 1;
	}
    
	return 0;
}

