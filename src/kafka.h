#ifndef KAFKA_H
#define KAFKA_H

#ifndef elog
#define elog(fmt, ...) fprintf(stderr, ("[%s:%d(%s)] " fmt), __FILE__, __LINE__, __PRETTY_FUNCTION__, ##__VA_ARGS__);
#endif

#include <librdkafka/rdkafka.h>

int init_kafka(char *brokers, char *querytopic, char *nxtopic, rd_kafka_t *rk, rd_kafka_topic_t *rkt_q, rd_kafka_topic_t *rkt_nx);
int send_query_data_to_kafka(rd_kafka_t *rk, rd_kafka_topic_t *rkt, char *kafkadata);
void shutdown_kafka(rd_kafka_t *rk, rd_kafka_topic_t *rkt_q, rd_kafka_topic_t *rkt_nx);

#endif /* KAFKA_H */
