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


/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is builtin from within the librdkafka source tree and thus differs. */

#include <librdkafka/rdkafka.h>

/**
 * Message delivery report callback using the richer rd_kafka_message_t object.
 */
static void msg_delivered (rd_kafka_t *rk,
                           const rd_kafka_message_t *rkmessage, void *opaque) {
        if (rkmessage->err)
		fprintf(stderr, "%% Message delivery failed: %s\n",
                        rd_kafka_err2str(rkmessage->err));

/* Note: commented out from example in librdkafka.
	
	else if (!quiet)
		fprintf(stderr,
                        "%% Message delivered (%zd bytes, offset %"PRId64", "
                        "partition %"PRId32"): %.*s\n",
                        rkmessage->len, rkmessage->offset,
			rkmessage->partition,
			(int)rkmessage->len, (const char *)rkmessage->payload);
*/

}

/**
 * 
 */ 
int send_query_data(rd_kafka_topic_t *rkt_q, char *kafkadata)
{
}

/**
 * 
 */ 
int send_nx_data(rd_kafka_topic_t *rkt_nx, char *kafkadata)
{

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

