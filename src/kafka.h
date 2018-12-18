#ifndef KAFKA_H
#define KAFKA_H

#include <librdkafka/rdkafka.h>

int init_kafka(char *brokers, char *querytopic, char *nxtopic, rd_kafka_t *rk, rd_kafka_topic_t *rkt_q, rd_kafka_topic_t *rkt_nx);
int send_query_data(rd_kafka_topic_t *rkt_q, char *kafkadata);
int send_nx_data(rd_kafka_topic_t *rkt_nx, char *kafkadata);

#endif /* KAFKA_H */
