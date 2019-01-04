#ifndef selog
#define selog(fac_pri, fmt, ...) { \
	openlog("PassiveDNS.kafka", LOG_CONS | LOG_PID | LOG_NDELAY, LOG_LOCAL1); \
	syslog(fac_pri, fmt, ##__VA_ARGS__); \
	closelog(); \
}
#endif