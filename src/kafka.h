// int init_kafka(char *brokers, char *querytopic, char *nxtopic, rd_kafka_t *rk, rd_kafka_topic_t *rkt_q, rd_kafka_topic_t *rkt_nx, uint8_t *broker_ready);
int init_kafka(struct _globalconfig *conf);
int send_query_data_to_kafka(struct _globalconfig *conf, uint8_t is_err_record, char *kafkadata, size_t len);
void shutdown_kafka(struct _globalconfig *conf);
void msg_delivered (rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque);
void error_cb (rd_kafka_t *rk, int err, const char *reason, void *opaque);
