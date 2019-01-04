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
#include <syslog.h>

#include <sys/types.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <pcap.h>
#include "passivedns.h"

#include "logging.h"
#include "kafka.h"

globalconfig config;
rd_kafka_conf_t *rk_conf;
rd_kafka_topic_conf_t *rkt_conf;
rd_kafka_topic_conf_t *rkt_nxd_conf;

/**
 * Message delivery report callback using the richer rd_kafka_message_t object.
 */
void msg_delivered (rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) 
{
	(void) opaque;
    if (rkmessage->err) {
		selog(LOG_ERR, "Kafka delivery failed: %s", rd_kafka_err2str(rkmessage->err));
		elog("[!] Message delivery failed: %s\n", rd_kafka_err2str(rkmessage->err));
	} else if (DEBUG) {
		/* */
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
int send_query_data_to_kafka(struct _globalconfig *conf, uint8_t is_err_record, char *kafkadata, size_t len)
{
	rd_kafka_topic_t 	*rkt_;

	retry:
		/* choose topic */
		if (is_err_record)
			rkt_ = conf->rkt_q;			
		else
			rkt_ = conf->rkt_nx;

		if (rd_kafka_produce(
            rkt_,					/* Topic object */
            RD_KAFKA_PARTITION_UA,  /* Use builtin partitioner to select partition*/
            RD_KAFKA_MSG_F_COPY,    /* Make a copy of the payload. */
            kafkadata, len, NULL, 0, NULL	/* Message payload (value) and length, Optional key and its length */
            ) == -1) {

                selog(LOG_ERR, "Failed to produce to topic %s: %s\n",	/* Failed to *enqueue* message for producing */
					rd_kafka_topic_name(rkt_),
                    rd_kafka_err2str(rd_kafka_last_error()));
				
				if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
					rd_kafka_poll(conf->rk, 1000); 	/* Max wait 1000 msec */
                    goto retry;
                }
                
			} else {
				rd_kafka_poll(conf->rk, 0); 			/* Non-blocking */
			}
}

void shutdown_kafka(struct _globalconfig *conf) 
{
	rd_kafka_flush(conf->rk, 5000); 					/* Max wait 5 sec */
	rd_kafka_topic_destroy(conf->rkt_q);
	rd_kafka_topic_destroy(conf->rkt_nx);
    rd_kafka_destroy(conf->rk);
    selog(LOG_NOTICE, "Kafka connection(s) closed.");
}

/* Initialize Kafka connection, return 0 if success
 */
int init_kafka(struct _globalconfig *conf) {

    char errstr[512];       /* librdkafka API error reporting buffer */

	rk_conf 		= rd_kafka_conf_new();
	rkt_conf 		= rd_kafka_topic_conf_new();
	rkt_nxd_conf 	= rd_kafka_topic_conf_new();
	
	// create kafka conf oject
	if (rd_kafka_conf_set (rk_conf, 
				"bootstrap.servers", 
				conf->kafka_broker, 
				errstr, 
				sizeof(errstr)) != RD_KAFKA_CONF_OK) {

        selog(LOG_ERR, "Kafka conf set error: %s", errstr);
        elog("[!] %s\n", errstr);
        return 1;
    }
    
	// set up delivery report callback
	rd_kafka_conf_set_dr_msg_cb(rk_conf, msg_delivered);
	// set up error callback
	rd_kafka_conf_set_error_cb(rk_conf, error_cb);
	
    // create producer instance
    if (!(conf->rk = rd_kafka_new (RD_KAFKA_PRODUCER, rk_conf, errstr, sizeof(errstr)))) {

        selog (LOG_ERR, "Error while kafka_new: %s", errstr);
        elog ("[!] Failed to create new producer: %s\n", errstr);
        return 1;
	}

	if (!(conf->rkt_q = rd_kafka_topic_new(conf->rk, conf->output_kafka_topic, rkt_conf))) {

		selog(LOG_ERR, "Error creating kafka querylog topic: %s\n",
			   rd_kafka_err2str(rd_kafka_last_error()));
		elog("[!] Failed to create querylog topic object: %s\n",
			   rd_kafka_err2str(rd_kafka_last_error()));
		return 1;

    } else {

		selog(LOG_INFO, "Kafka querylog topic created: %s", conf->output_kafka_topic);
		olog("Querylog topic object created: %s\n", conf->output_kafka_topic);
		conf->kafka_broker_ready = 1;
	}

	if (!(conf->rkt_nx = rd_kafka_topic_new(conf->rk, conf->output_kafka_topic, rkt_nxd_conf))) {

		selog(LOG_ERR, "Error creating kafka nxlog topic: %s\n",
			   rd_kafka_err2str(rd_kafka_last_error()));
		elog("[!] Failed to create topic object: %s\n",
				rd_kafka_err2str(rd_kafka_last_error()));
		return 1;

    } else {

		selog(LOG_INFO, "Kafka nxlog topic created: %s", conf->output_kafka_topic);
		olog("Topic object created: %s\n", conf->output_kafka_topic);
		conf->kafka_broker_ready = 1;
	}
    
	return 0;
}

