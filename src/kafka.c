/**
 * Send passive dns feed to the kafka cluster
 *   requires: broker and 2 topics (queries, NXDOMAIN)
 * 
 * Simple Apache Kafka producer
 * using the Kafka driver from librdkafka
 * (https://github.com/edenhill/librdkafka)
 */

#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <librdkafka/rdkafka.h>

#include "kafka.h"

/* internal */
static void msg_delivered (rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque);

/* functions */
/**
 * Message delivery report callback using the richer rd_kafka_message_t object.
 */
static void msg_delivered (rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) 
{
    if (rkmessage->err)
	elog("[!] Message delivery failed: %s\n", rd_kafka_err2str(rkmessage->err));
}

/**
 * Produce message
 */ 
int send_query_data_to_kafka(rd_kafka_t *rk, rd_kafka_topic_t *rkt, char *kafkadata)
{
	int len;
	
	len = strlen(kafkadata);
	
	retry:  
		if (rd_kafka_produce(
            rkt, 					/* Topic object */
            RD_KAFKA_PARTITION_UA,  /* Use builtin partitioner to select partition*/
            RD_KAFKA_MSG_F_COPY,    /* Make a copy of the payload. */
            kafkadata, len, NULL, 0, NULL	/* Message payload (value) and length, Optional key and its length */
            ) == -1) {
				elog( "[!] Failed to produce to topic %s: %s\n",	/* Failed to *enqueue* message for producing */
                    rd_kafka_topic_name(rkt),
                    rd_kafka_err2str(rd_kafka_last_error()));
				
				if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
					rd_kafka_poll(rk, 1000); 	/* Max wait 1000 msec */
                    goto retry;
                }
                
			} else {
				rd_kafka_poll(rk, 0); 			/* Non-blocking */
			}
}

void shutdown_kafka(rd_kafka_t *rk, rd_kafka_topic_t *rkt_q, rd_kafka_topic_t *rkt_nx) 
{
	rd_kafka_flush(rk, 10*1000); 				/* Max wait 10 sec */
	rd_kafka_topic_destroy(rkt_q);
	rd_kafka_topic_destroy(rkt_nx);
    rd_kafka_destroy(rk);
}

/* Initialize Kafka connection, return 0 if success
 */
int init_kafka(char *brokers, char *querytopic, char *nxtopic, 
	rd_kafka_t *rk, rd_kafka_topic_t *rkt_q, rd_kafka_topic_t *rkt_nx) 
{
    rd_kafka_conf_t *conf;  /* Temporary configuration object */
    char errstr[512];       /* librdkafka API error reporting buffer */

	conf = rd_kafka_conf_new();
	
	// create kafka conf oject
	if (rd_kafka_conf_set(conf, 
				"bootstrap.servers", 
				brokers, 
				errstr, 
				sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%s\n", errstr);
        return 1;
    }
    
	// set up delivery report callback
	rd_kafka_conf_set_dr_msg_cb(conf, 
		msg_delivered);
	
    // create producer instance
    rk = rd_kafka_new(RD_KAFKA_PRODUCER, 
    			conf, 
    			errstr, 
    			sizeof(errstr));
    if (!rk) {
        fprintf(stderr,
            "%% Failed to create new producer: %s\n", errstr);
        return 1;
	}

	rkt_q = rd_kafka_topic_new(rk, 
			querytopic, 
			NULL);
    if (!rkt_q) {
            fprintf(stderr, "%% Failed to create topic object: %s\n",
                    rd_kafka_err2str(rd_kafka_last_error()));
            rd_kafka_destroy(rk);
            return 1;
    }

	rkt_nx = rd_kafka_topic_new(rk, 
			nxtopic,
			NULL);
    if (!rkt_nx) {
            fprintf(stderr, "%% Failed to create topic object: %s\n",
                    rd_kafka_err2str(rd_kafka_last_error()));
            rd_kafka_destroy(rk);
            return 1;
    }
	
	return 0;

}

