#include <sys/resource.h>
#include <locale.h>
#include "server.h"

#include "ae.h"
#include "sds.h"
#include "dict.h"
#include "anet.h"
#include "util.h"
#include "config.h"
#include "zmalloc.h"

uint64_t dictSdsCaseHash(const void *key) {
	return dictGenCaseHashFunction((unsigned char*)key, sdslen((char*)key));
}

int dictSdsKeyCaseCompare(dict *d, const void *key1,
	const void *key2)
{
	UNUSED(d);
	return strcasecmp(key1, key2) == 0;
}

void dictSdsDestructor(dict *d, void *val)
{
	UNUSED(d);
	sdsfree(val);
}

/* Command table. sds string -> command struct pointer. */
dictType commandTableDictType = {
    dictSdsCaseHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCaseCompare,      /* key compare */
    dictSdsDestructor,          /* key destructor */
    NULL,                       /* val destructor */
    NULL                        /* allow to expand */
};

/* Add a sample to the operations per second array of samples. */
void trackInstantaneousMetric(int metric, long long current_reading) {
    long long now = mstime();
    long long t = now - server.inst_metric[metric].last_sample_time;
    long long ops = current_reading -
                    server.inst_metric[metric].last_sample_count;
    long long ops_sec;

    ops_sec = t > 0 ? (ops*1000/t) : 0;

    server.inst_metric[metric].samples[server.inst_metric[metric].idx] =
        ops_sec;
    server.inst_metric[metric].idx++;
    server.inst_metric[metric].idx %= STATS_METRIC_SAMPLES;
    server.inst_metric[metric].last_sample_time = now;
    server.inst_metric[metric].last_sample_count = current_reading;
}

/* Return the mean of all the samples. */
long long getInstantaneousMetric(int metric) {
    int j;
    long long sum = 0;

    for (j = 0; j < STATS_METRIC_SAMPLES; j++)
        sum += server.inst_metric[metric].samples[j];
    return sum / STATS_METRIC_SAMPLES;
}


/* The client query buffer is an sds.c string that can end with a lot of
 * free space not used, this function reclaims space if needed.
 *
 * The function always returns 0 as it never terminates the client. */
int clientsCronResizeQueryBuffer(client *c) {
	size_t querybuf_size = sdsalloc(c->querybuf);
	time_t idletime = server.unixtime - c->lastinteraction;

	/* Only resize the query buffer if the buffer is actually wasting at least a
	 * few kbytes */
	if (sdsavail(c->querybuf) > 1024 * 4) {
		/* There are two conditions to resize the query buffer: */
		if (idletime > 2) {
			/* 1) Query is idle for a long time. */
			c->querybuf = sdsRemoveFreeSpace(c->querybuf, 1);
		}
		else if (querybuf_size > PROTO_RESIZE_THRESHOLD && querybuf_size / 2 > c->querybuf_peak) {
			/* 2) Query buffer is too big for latest peak and is larger than
			 *    resize threshold. Trim excess space but only up to a limit,
			 *    not below the recent peak and current c->querybuf (which will
			 *    be soon get used). If we're in the middle of a bulk then make
			 *    sure not to resize to less than the bulk length. */
			size_t resize = sdslen(c->querybuf);
			if (resize < c->querybuf_peak) resize = c->querybuf_peak;
			if (c->bulklen != -1 && resize < (size_t)c->bulklen) resize = c->bulklen;
			c->querybuf = sdsResize(c->querybuf, resize, 1);
		}
	}

	/* Reset the peak again to capture the peak memory usage in the next
	 * cycle. */
	c->querybuf_peak = sdslen(c->querybuf);
	/* We reset to either the current used, or currently processed bulk size,
	 * which ever is bigger. */
	if (c->bulklen != -1 && (size_t)c->bulklen > c->querybuf_peak)
		c->querybuf_peak = c->bulklen;
	return 0;
}

/* The client output buffer can be adjusted to better fit the memory requirements.
 *
 * the logic is:
 * in case the last observed peak size of the buffer equals the buffer size - we double the size
 * in case the last observed peak size of the buffer is less than half the buffer size - we shrink by half.
 * The buffer peak will be reset back to the buffer position every server.reply_buffer_peak_reset_time milliseconds
 * The function always returns 0 as it never terminates the client. */
int clientsCronResizeOutputBuffer(client *c, mstime_t now_ms) {

	size_t new_buffer_size = 0;
	char *oldbuf = NULL;
	const size_t buffer_target_shrink_size = c->buf_usable_size / 2;
	const size_t buffer_target_expand_size = c->buf_usable_size * 2;

	/* in case the resizing is disabled return immediately */
	if (!server.reply_buffer_resizing_enabled)
		return 0;

	if (buffer_target_shrink_size >= PROTO_REPLY_MIN_BYTES &&
		c->buf_peak < buffer_target_shrink_size)
	{
		new_buffer_size = max(PROTO_REPLY_MIN_BYTES, c->buf_peak + 1);
		server.stat_reply_buffer_shrinks++;
	}
	else if (buffer_target_expand_size < PROTO_REPLY_CHUNK_BYTES * 2 &&
		c->buf_peak == c->buf_usable_size)
	{
		new_buffer_size = min(PROTO_REPLY_CHUNK_BYTES, buffer_target_expand_size);
		server.stat_reply_buffer_expands++;
	}

	/* reset the peak value each server.reply_buffer_peak_reset_time seconds. in case the client will be idle
	 * it will start to shrink.
	 */
	if (server.reply_buffer_peak_reset_time >= 0 &&
		now_ms - c->buf_peak_last_reset_time >= server.reply_buffer_peak_reset_time)
	{
		c->buf_peak = c->bufpos;
		c->buf_peak_last_reset_time = now_ms;
	}

	if (new_buffer_size) {
		oldbuf = c->buf;
		c->buf = zmalloc_usable(new_buffer_size, &c->buf_usable_size);
		memcpy(c->buf, oldbuf, c->bufpos);
		zfree(oldbuf);
	}
	return 0;
}

/* This function is used in order to track clients using the biggest amount
 * of memory in the latest few seconds. This way we can provide such information
 * in the INFO output (clients section), without having to do an O(N) scan for
 * all the clients.
 *
 * This is how it works. We have an array of CLIENTS_PEAK_MEM_USAGE_SLOTS slots
 * where we track, for each, the biggest client output and input buffers we
 * saw in that slot. Every slot corresponds to one of the latest seconds, since
 * the array is indexed by doing UNIXTIME % CLIENTS_PEAK_MEM_USAGE_SLOTS.
 *
 * When we want to know what was recently the peak memory usage, we just scan
 * such few slots searching for the maximum value. */
#define CLIENTS_PEAK_MEM_USAGE_SLOTS 8
size_t ClientsPeakMemInput[CLIENTS_PEAK_MEM_USAGE_SLOTS] = { 0 };
size_t ClientsPeakMemOutput[CLIENTS_PEAK_MEM_USAGE_SLOTS] = { 0 };

int clientsCronTrackExpansiveClients(client *c, int time_idx) {
	size_t in_usage = sdsZmallocSize(c->querybuf) + c->argv_len_sum +
		(c->argv ? zmalloc_size(c->argv) : 0);
	size_t out_usage = getClientOutputBufferMemoryUsage(c);

	/* Track the biggest values observed so far in this slot. */
	if (in_usage > ClientsPeakMemInput[time_idx]) ClientsPeakMemInput[time_idx] = in_usage;
	if (out_usage > ClientsPeakMemOutput[time_idx]) ClientsPeakMemOutput[time_idx] = out_usage;

	return 0; /* This function never terminates the client. */
}

/* All normal clients are placed in one of the "mem usage buckets" according
 * to how much memory they currently use. We use this function to find the
 * appropriate bucket based on a given memory usage value. The algorithm simply
 * does a log2(mem) to ge the bucket. This means, for examples, that if a
 * client's memory usage doubles it's moved up to the next bucket, if it's
 * halved we move it down a bucket.
 * For more details see CLIENT_MEM_USAGE_BUCKETS documentation in server.h. */
static inline clientMemUsageBucket *getMemUsageBucket(size_t mem) {
	int size_in_bits = 8 * (int)sizeof(mem);
	int clz = mem > 0 ? __builtin_clzl(mem) : size_in_bits;
	int bucket_idx = size_in_bits - clz;
	if (bucket_idx > CLIENT_MEM_USAGE_BUCKET_MAX_LOG)
		bucket_idx = CLIENT_MEM_USAGE_BUCKET_MAX_LOG;
	else if (bucket_idx < CLIENT_MEM_USAGE_BUCKET_MIN_LOG)
		bucket_idx = CLIENT_MEM_USAGE_BUCKET_MIN_LOG;
	bucket_idx -= CLIENT_MEM_USAGE_BUCKET_MIN_LOG;
	return &server.client_mem_usage_buckets[bucket_idx];
}

/*
 * This method updates the client memory usage and update the
 * server stats for client type.
 *
 * This method is called from the clientsCron to have updated
 * stats for non CLIENT_TYPE_NORMAL/PUBSUB clients to accurately
 * provide information around clients memory usage.
 *
 * It is also used in updateClientMemUsageAndBucket to have latest
 * client memory usage information to place it into appropriate client memory
 * usage bucket.
 */
void updateClientMemoryUsage(client *c) {
	size_t mem = getClientMemoryUsage(c, NULL);
	int type = getClientType(c);
	/* Now that we have the memory used by the client, remove the old
	 * value from the old category, and add it back. */
	server.stat_clients_type_memory[c->last_memory_type] -= c->last_memory_usage;
	server.stat_clients_type_memory[type] += mem;
	/* Remember what we added and where, to remove it next time. */
	c->last_memory_type = type;
	c->last_memory_usage = mem;
}

int clientEvictionAllowed(client *c) {
	if (server.maxmemory_clients == 0 || c->flags & CLIENT_NO_EVICT) {
		return 0;
	}
	int type = getClientType(c);
	return (type == CLIENT_TYPE_NORMAL || type == CLIENT_TYPE_PUBSUB);
}


/* This function is used to cleanup the client's previously tracked memory usage.
 * This is called during incremental client memory usage tracking as well as
 * used to reset when client to bucket allocation is not required when
 * client eviction is disabled.  */
void removeClientFromMemUsageBucket(client *c, int allow_eviction) {
	if (c->mem_usage_bucket) {
		c->mem_usage_bucket->mem_usage_sum -= c->last_memory_usage;
		/* If this client can't be evicted then remove it from the mem usage
		 * buckets */
		if (!allow_eviction) {
			listDelNode(c->mem_usage_bucket->clients, c->mem_usage_bucket_node);
			c->mem_usage_bucket = NULL;
			c->mem_usage_bucket_node = NULL;
		}
	}
}

/* This is called only if explicit clients when something changed their buffers,
 * so we can track clients' memory and enforce clients' maxmemory in real time.
 *
 * This also adds the client to the correct memory usage bucket. Each bucket contains
 * all clients with roughly the same amount of memory. This way we group
 * together clients consuming about the same amount of memory and can quickly
 * free them in case we reach maxmemory-clients (client eviction).
 *
 * Note: This function filters clients of type monitor, master or replica regardless
 * of whether the eviction is enabled or not, so the memory usage we get from these
 * types of clients via the INFO command may be out of date. If someday we wanna
 * improve that to make monitors' memory usage more accurate, we need to re-add this
 * function call to `replicationFeedMonitors()`.
 *
 * returns 1 if client eviction for this client is allowed, 0 otherwise.
 */
int updateClientMemUsageAndBucket(client *c) {
	serverAssert(io_threads_op == IO_THREADS_OP_IDLE);
	int allow_eviction = clientEvictionAllowed(c);
	removeClientFromMemUsageBucket(c, allow_eviction);

	if (!allow_eviction) {
		return 0;
	}

	/* Update client memory usage. */
	updateClientMemoryUsage(c);

	/* Update the client in the mem usage buckets */
	clientMemUsageBucket *bucket = getMemUsageBucket(c->last_memory_usage);
	bucket->mem_usage_sum += c->last_memory_usage;
	if (bucket != c->mem_usage_bucket) {
		if (c->mem_usage_bucket)
			listDelNode(c->mem_usage_bucket->clients,
				c->mem_usage_bucket_node);
		c->mem_usage_bucket = bucket;
		listAddNodeTail(bucket->clients, c);
		c->mem_usage_bucket_node = listLast(bucket->clients);
	}
	return 1;
}

/* Return the max samples in the memory usage of clients tracked by
 * the function clientsCronTrackExpansiveClients(). */
void getExpansiveClientsInfo(size_t *in_usage, size_t *out_usage) {
	size_t i = 0, o = 0;
	for (int j = 0; j < CLIENTS_PEAK_MEM_USAGE_SLOTS; j++) {
		if (ClientsPeakMemInput[j] > i) i = ClientsPeakMemInput[j];
		if (ClientsPeakMemOutput[j] > o) o = ClientsPeakMemOutput[j];
	}
	*in_usage = i;
	*out_usage = o;
}

/* This function is called by serverCron() and is used in order to perform
 * operations on clients that are important to perform constantly. For instance
 * we use this function in order to disconnect clients after a timeout, including
 * clients blocked in some blocking command with a non-zero timeout.
 *
 * The function makes some effort to process all the clients every second, even
 * if this cannot be strictly guaranteed, since serverCron() may be called with
 * an actual frequency lower than server.hz in case of latency events like slow
 * commands.
 *
 * It is very important for this function, and the functions it calls, to be
 * very fast: sometimes Redis has tens of hundreds of connected clients, and the
 * default server.hz value is 10, so sometimes here we need to process thousands
 * of clients per second, turning this function into a source of latency.
 */
#define CLIENTS_CRON_MIN_ITERATIONS 5
void clientsCron(void) {
	/* Try to process at least numclients/server.hz of clients
	 * per call. Since normally (if there are no big latency events) this
	 * function is called server.hz times per second, in the average case we
	 * process all the clients in 1 second. */
	int numclients = listLength(server.clients);
	int iterations = numclients / server.hz;
	mstime_t now = mstime();

	/* Process at least a few clients while we are at it, even if we need
	 * to process less than CLIENTS_CRON_MIN_ITERATIONS to meet our contract
	 * of processing each client once per second. */
	if (iterations < CLIENTS_CRON_MIN_ITERATIONS)
		iterations = (numclients < CLIENTS_CRON_MIN_ITERATIONS) ?
		numclients : CLIENTS_CRON_MIN_ITERATIONS;


	int curr_peak_mem_usage_slot = server.unixtime % CLIENTS_PEAK_MEM_USAGE_SLOTS;
	/* Always zero the next sample, so that when we switch to that second, we'll
	 * only register samples that are greater in that second without considering
	 * the history of such slot.
	 *
	 * Note: our index may jump to any random position if serverCron() is not
	 * called for some reason with the normal frequency, for instance because
	 * some slow command is called taking multiple seconds to execute. In that
	 * case our array may end containing data which is potentially older
	 * than CLIENTS_PEAK_MEM_USAGE_SLOTS seconds: however this is not a problem
	 * since here we want just to track if "recently" there were very expansive
	 * clients from the POV of memory usage. */
	int zeroidx = (curr_peak_mem_usage_slot + 1) % CLIENTS_PEAK_MEM_USAGE_SLOTS;
	ClientsPeakMemInput[zeroidx] = 0;
	ClientsPeakMemOutput[zeroidx] = 0;


	while (listLength(server.clients) && iterations--) {
		client *c;
		listNode *head;

		/* Rotate the list, take the current head, process.
		 * This way if the client must be removed from the list it's the
		 * first element and we don't incur into O(N) computation. */
		listRotateTailToHead(server.clients);
		head = listFirst(server.clients);
		c = listNodeValue(head);
		/* The following functions do different service checks on the client.
		 * The protocol is that they return non-zero if the client was
		 * terminated. */
		if (clientsCronHandleTimeout(c, now)) continue;
		if (clientsCronResizeQueryBuffer(c)) continue;
		if (clientsCronResizeOutputBuffer(c, now)) continue;

		if (clientsCronTrackExpansiveClients(c, curr_peak_mem_usage_slot)) continue;

		/* Iterating all the clients in getMemoryOverheadData() is too slow and
		 * in turn would make the INFO command too slow. So we perform this
		 * computation incrementally and track the (not instantaneous but updated
		 * to the second) total memory used by clients using clientsCron() in
		 * a more incremental way (depending on server.hz).
		 * If client eviction is enabled, update the bucket as well. */
		if (!updateClientMemUsageAndBucket(c))
			updateClientMemoryUsage(c);

		if (closeClientOnOutputBufferLimitReached(c, 0)) continue;
	}
}



static inline void updateCachedTimeWithUs(int update_daylight_info, const long long ustime) {
	server.ustime = ustime;
	server.mstime = server.ustime / 1000;
	time_t unixtime = server.mstime / 1000;
	atomicSet(server.unixtime, unixtime);

	/* To get information about daylight saving time, we need to call
	 * localtime_r and cache the result. However calling localtime_r in this
	 * context is safe since we will never fork() while here, in the main
	 * thread. The logging function will call a thread safe version of
	 * localtime that has no locks. */
	if (update_daylight_info) {
		struct tm tm;
		time_t ut = server.unixtime;
		localtime_r(&ut, &tm);
		server.daylight_active = tm.tm_isdst;
	}
}

/* We take a cached value of the unix time in the global state because with
 * virtual memory and aging there is to store the current time in objects at
 * every object access, and accuracy is not needed. To access a global var is
 * a lot faster than calling time(NULL).
 *
 * This function should be fast because it is called at every command execution
 * in call(), so it is possible to decide if to update the daylight saving
 * info or not using the 'update_daylight_info' argument. Normally we update
 * such info only when calling this function from serverCron() but not when
 * calling it from call(). */
void updateCachedTime(int update_daylight_info) {
	const long long us = ustime();
	updateCachedTimeWithUs(update_daylight_info, us);
}


/* Called from serverCron and cronUpdateMemoryStats to update cached memory metrics. */
void cronUpdateMemoryStats() {
	/* Record the max memory used since the server was started. */
	if (zmalloc_used_memory() > server.stat_peak_memory)
		server.stat_peak_memory = zmalloc_used_memory();

	run_with_period(100) {
		/* Sample the RSS and other metrics here since this is a relatively slow call.
		 * We must sample the zmalloc_used at the same time we take the rss, otherwise
		 * the frag ratio calculate may be off (ratio of two samples at different times) */
		server.cron_malloc_stats.process_rss = zmalloc_get_rss();
		server.cron_malloc_stats.zmalloc_used = zmalloc_used_memory();
		/* Sampling the allocator info can be slow too.
		 * The fragmentation ratio it'll show is potentially more accurate
		 * it excludes other RSS pages such as: shared libraries, LUA and other non-zmalloc
		 * allocations, and allocator reserved pages that can be pursed (all not actual frag) */
		zmalloc_get_allocator_info(&server.cron_malloc_stats.allocator_allocated,
			&server.cron_malloc_stats.allocator_active,
			&server.cron_malloc_stats.allocator_resident);
		/* in case the allocator isn't providing these stats, fake them so that
		 * fragmentation info still shows some (inaccurate metrics) */
		if (!server.cron_malloc_stats.allocator_active)
			server.cron_malloc_stats.allocator_active = server.cron_malloc_stats.allocator_resident;
		if (!server.cron_malloc_stats.allocator_allocated)
			server.cron_malloc_stats.allocator_allocated = server.cron_malloc_stats.zmalloc_used;
	}
}

/* This is our timer interrupt, called server.hz times per second.
 * Here is where we do a number of things that need to be done asynchronously.
 * For instance:
 *
 * - Active expired keys collection (it is also performed in a lazy way on
 *   lookup).
 * - Software watchdog.
 * - Update some statistic.
 * - Incremental rehashing of the DBs hash tables.
 * - Triggering BGSAVE / AOF rewrite, and handling of terminated children.
 * - Clients timeout of different kinds.
 * - Replication reconnection.
 * - Many more...
 *
 * Everything directly called here will be called server.hz times per second,
 * so in order to throttle execution of things we want to do less frequently
 * a macro is used: run_with_period(milliseconds) { .... }
 */

int serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData) {
	int j;
	UNUSED(eventLoop);
	UNUSED(id);
	UNUSED(clientData);

	/* Software watchdog: deliver the SIGALRM that will reach the signal
	 * handler if we don't return here fast enough. */
	if (server.watchdog_period) watchdogScheduleSignal(server.watchdog_period);

	/* Update the time cache. */
	updateCachedTime(1);

	server.hz = server.config_hz;
	/* Adapt the server.hz value to the number of configured clients. If we have
	 * many clients, we want to call serverCron() with an higher frequency. */
	if (server.dynamic_hz) {
		while (listLength(server.clients) / server.hz >
			MAX_CLIENTS_PER_CLOCK_TICK)
		{
			server.hz *= 2;
			if (server.hz > CONFIG_MAX_HZ) {
				server.hz = CONFIG_MAX_HZ;
				break;
			}
		}
	}

	/* for debug purposes: skip actual cron work if pause_cron is on */
	if (server.pause_cron) return 1000 / server.hz;

	run_with_period(100) {
		long long stat_net_input_bytes, stat_net_output_bytes;
		atomicGet(server.stat_net_input_bytes, stat_net_input_bytes);
		atomicGet(server.stat_net_output_bytes, stat_net_output_bytes);

		trackInstantaneousMetric(STATS_METRIC_COMMAND, server.stat_numcommands);
	}

	/* We have just LRU_BITS bits per object for LRU information.
	 * So we use an (eventually wrapping) LRU clock.
	 *
	 * Note that even if the counter wraps it's not a big problem,
	 * everything will still work but some object will appear younger
	 * to Redis. However for this to happen a given object should never be
	 * touched for all the time needed to the counter to wrap, which is
	 * not likely.
	 *
	 * Note that you can change the resolution altering the
	 * LRU_CLOCK_RESOLUTION define. */
	unsigned int lruclock = getLRUClock();
	atomicSet(server.lruclock, lruclock);

	cronUpdateMemoryStats();

	/* We need to do a few operations on clients asynchronously. */
	clientsCron();

	/* Clear the paused clients state if needed. */
	checkClientPauseTimeoutAndReturnIfPaused();

	/* Resize tracking keys table if needed. This is also done at every
	 * command execution, but we want to be sure that if the last command
	 * executed changes the value via CONFIG SET, the server will perform
	 * the operation even if completely idle. */
	if (server.tracking_clients) trackingLimitUsedSlots();

	server.cronloops++;
	return 1000 / server.hz;
}


void blockingOperationStarts() {
	if (!server.blocking_op_nesting++) {
		updateCachedTime(0);
		server.blocked_last_cron = server.mstime;
	}
}

void blockingOperationEnds() {
	if (!(--server.blocking_op_nesting)) {
		server.blocked_last_cron = 0;
	}
}

/* This function fills in the role of serverCron during RDB or AOF loading, and
 * also during blocked scripts.
 * It attempts to do its duties at a similar rate as the configured server.hz,
 * and updates cronloops variable so that similarly to serverCron, the
 * run_with_period can be used. */
void whileBlockedCron() {
	/* Here we may want to perform some cron jobs (normally done server.hz times
	 * per second). */

	 /* Since this function depends on a call to blockingOperationStarts, let's
	  * make sure it was done. */
	serverAssert(server.blocked_last_cron);

	/* In case we where called too soon, leave right away. This way one time
	 * jobs after the loop below don't need an if. and we don't bother to start
	 * latency monitor if this function is called too often. */
	if (server.blocked_last_cron >= server.mstime)
		return;

	mstime_t latency;
	latencyStartMonitor(latency);

	/* In some cases we may be called with big intervals, so we may need to do
	 * extra work here. This is because some of the functions in serverCron rely
	 * on the fact that it is performed every 10 ms or so. For instance, if
	 * activeDefragCycle needs to utilize 25% cpu, it will utilize 2.5ms, so we
	 * need to call it multiple times. */
	long hz_ms = 1000 / server.hz;
	while (server.blocked_last_cron < server.mstime) {

		/* Defrag keys gradually. */
		activeDefragCycle();

		server.blocked_last_cron += hz_ms;

		/* Increment cronloop so that run_with_period works. */
		server.cronloops++;
	}

	/* Other cron jobs do not need to be done in a loop. No need to check
	 * server.blocked_last_cron since we have an early exit at the top. */

	 /* Update memory stats during loading (excluding blocked scripts) */
	if (server.loading) cronUpdateMemoryStats();

	latencyEndMonitor(latency);
	latencyAddSampleIfNeeded("while-blocked-cron", latency);

	/* We received a SIGTERM during loading, shutting down here in a safe way,
	 * as it isn't ok doing so inside the signal handler. */
	if (server.shutdown_asap && server.loading) {
		if (prepareForShutdown(SHUTDOWN_NOSAVE) == C_OK) exit(0);
		serverLog(LL_WARNING, "SIGTERM received but errors trying to shut down the server, check the logs for more information");
		server.shutdown_asap = 0;
		server.last_sig_received = 0;
	}
}


extern int ProcessingEventsWhileBlocked;

/* This function gets called every time Redis is entering the
 * main loop of the event driven library, that is, before to sleep
 * for ready file descriptors.
 *
 * Note: This function is (currently) called from two functions:
 * 1. aeMain - The main server loop
 * 2. processEventsWhileBlocked - Process clients during RDB/AOF load
 *
 * If it was called from processEventsWhileBlocked we don't want
 * to perform all actions (For example, we don't want to expire
 * keys), but we do need to perform some actions.
 *
 * The most important is freeClientsInAsyncFreeQueue but we also
 * call some other low-risk functions. */
void beforeSleep(struct aeEventLoop *eventLoop) {
	UNUSED(eventLoop);

	size_t zmalloc_used = zmalloc_used_memory();
	if (zmalloc_used > server.stat_peak_memory)
		server.stat_peak_memory = zmalloc_used;

	/* Just call a subset of vital functions in case we are re-entering
	 * the event loop from processEventsWhileBlocked(). Note that in this
	 * case we keep track of the number of events we are processing, since
	 * processEventsWhileBlocked() wants to stop ASAP if there are no longer
	 * events to handle. */
	if (ProcessingEventsWhileBlocked) {
		uint64_t processed = 0;
		processed += handleClientsWithPendingReadsUsingThreads();
		processed += tlsProcessPendingData();
		processed += handleClientsWithPendingWrites();
		processed += freeClientsInAsyncFreeQueue();
		server.events_processed_while_blocked += processed;
		return;
	}

	/* Handle precise timeouts of blocked clients. */
	handleBlockedClientsTimeout();

	/* We should handle pending reads clients ASAP after event loop. */
	handleClientsWithPendingReadsUsingThreads();

	/* Handle TLS pending data. (must be done before flushAppendOnlyFile) */
	tlsProcessPendingData();

	/* If tls still has pending unread data don't sleep at all. */
	aeSetDontWait(server.el, tlsHasPendingData());

	/* Try to process pending commands for clients that were just unblocked. */
	if (listLength(server.unblocked_clients))
		processUnblockedClients();


	/* Since we rely on current_client to send scheduled invalidation messages
	 * we have to flush them after each command, so when we get here, the list
	 * must be empty. */
	serverAssert(listLength(server.tracking_pending_keys) == 0);

	/* Send the invalidation messages to clients participating to the
	 * client side caching protocol in broadcasting (BCAST) mode. */
	trackingBroadcastInvalidationMessages();

	/* Try to process blocked clients every once in while.
	 *
	 * Example: A module calls RM_SignalKeyAsReady from within a timer callback
	 * (So we don't visit processCommand() at all).
	 *
	 * must be done before flushAppendOnlyFile, in case of appendfsync=always,
	 * since the unblocked clients may write data. */
	handleClientsBlockedOnKeys();

	/* Handle writes with pending output buffers. */
	handleClientsWithPendingWritesUsingThreads();

	/* Close clients that need to be closed asynchronous */
	freeClientsInAsyncFreeQueue();

	/* Disconnect some clients if they are consuming too much memory. */
	evictClients();
}

/* This function is called immediately after the event loop multiplexing
 * API returned, and the control is going to soon return to Redis by invoking
 * the different events callbacks. */
void afterSleep(struct aeEventLoop *eventLoop) {
	UNUSED(eventLoop);

	/* Do NOT add anything above moduleAcquireGIL !!! */
}

/* =========================== Server initialization ======================== */

void createSharedObjects(void) {
	int j;

	/* Shared command responses */
	shared.crlf = createObject(OBJ_STRING, sdsnew("\r\n"));
	shared.ok = createObject(OBJ_STRING, sdsnew("+OK\r\n"));
	shared.emptybulk = createObject(OBJ_STRING, sdsnew("$0\r\n\r\n"));
	shared.czero = createObject(OBJ_STRING, sdsnew(":0\r\n"));
	shared.cone = createObject(OBJ_STRING, sdsnew(":1\r\n"));
	shared.emptyarray = createObject(OBJ_STRING, sdsnew("*0\r\n"));
	shared.pong = createObject(OBJ_STRING, sdsnew("+PONG\r\n"));
	shared.queued = createObject(OBJ_STRING, sdsnew("+QUEUED\r\n"));
	shared.emptyscan = createObject(OBJ_STRING, sdsnew("*2\r\n$1\r\n0\r\n*0\r\n"));
	shared.space = createObject(OBJ_STRING, sdsnew(" "));
	shared.plus = createObject(OBJ_STRING, sdsnew("+"));

	/* Shared command error responses */
	shared.wrongtypeerr = createObject(OBJ_STRING, sdsnew(
		"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"));
	shared.err = createObject(OBJ_STRING, sdsnew("-ERR\r\n"));
	shared.nokeyerr = createObject(OBJ_STRING, sdsnew(
		"-ERR no such key\r\n"));
	shared.syntaxerr = createObject(OBJ_STRING, sdsnew(
		"-ERR syntax error\r\n"));
	shared.sameobjecterr = createObject(OBJ_STRING, sdsnew(
		"-ERR source and destination objects are the same\r\n"));
	shared.outofrangeerr = createObject(OBJ_STRING, sdsnew(
		"-ERR index out of range\r\n"));
	shared.noscripterr = createObject(OBJ_STRING, sdsnew(
		"-NOSCRIPT No matching script. Please use EVAL.\r\n"));
	shared.loadingerr = createObject(OBJ_STRING, sdsnew(
		"-LOADING Redis is loading the dataset in memory\r\n"));
	shared.slowevalerr = createObject(OBJ_STRING, sdsnew(
		"-BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.\r\n"));
	shared.slowscripterr = createObject(OBJ_STRING, sdsnew(
		"-BUSY Redis is busy running a script. You can only call FUNCTION KILL or SHUTDOWN NOSAVE.\r\n"));
	shared.slowmoduleerr = createObject(OBJ_STRING, sdsnew(
		"-BUSY Redis is busy running a module command.\r\n"));
	shared.masterdownerr = createObject(OBJ_STRING, sdsnew(
		"-MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'.\r\n"));
	shared.bgsaveerr = createObject(OBJ_STRING, sdsnew(
		"-MISCONF Redis is configured to save RDB snapshots, but it's currently unable to persist to disk. Commands that may modify the data set are disabled, because this instance is configured to report errors during writes if RDB snapshotting fails (stop-writes-on-bgsave-error option). Please check the Redis logs for details about the RDB error.\r\n"));
	shared.roslaveerr = createObject(OBJ_STRING, sdsnew(
		"-READONLY You can't write against a read only replica.\r\n"));
	shared.noautherr = createObject(OBJ_STRING, sdsnew(
		"-NOAUTH Authentication required.\r\n"));
	shared.oomerr = createObject(OBJ_STRING, sdsnew(
		"-OOM command not allowed when used memory > 'maxmemory'.\r\n"));
	shared.execaborterr = createObject(OBJ_STRING, sdsnew(
		"-EXECABORT Transaction discarded because of previous errors.\r\n"));
	shared.noreplicaserr = createObject(OBJ_STRING, sdsnew(
		"-NOREPLICAS Not enough good replicas to write.\r\n"));
	shared.busykeyerr = createObject(OBJ_STRING, sdsnew(
		"-BUSYKEY Target key name already exists.\r\n"));

	/* The shared NULL depends on the protocol version. */
	shared.null[0] = NULL;
	shared.null[1] = NULL;
	shared.null[2] = createObject(OBJ_STRING, sdsnew("$-1\r\n"));
	shared.null[3] = createObject(OBJ_STRING, sdsnew("_\r\n"));

	shared.nullarray[0] = NULL;
	shared.nullarray[1] = NULL;
	shared.nullarray[2] = createObject(OBJ_STRING, sdsnew("*-1\r\n"));
	shared.nullarray[3] = createObject(OBJ_STRING, sdsnew("_\r\n"));

	shared.emptymap[0] = NULL;
	shared.emptymap[1] = NULL;
	shared.emptymap[2] = createObject(OBJ_STRING, sdsnew("*0\r\n"));
	shared.emptymap[3] = createObject(OBJ_STRING, sdsnew("%0\r\n"));

	shared.emptyset[0] = NULL;
	shared.emptyset[1] = NULL;
	shared.emptyset[2] = createObject(OBJ_STRING, sdsnew("*0\r\n"));
	shared.emptyset[3] = createObject(OBJ_STRING, sdsnew("~0\r\n"));

	for (j = 0; j < PROTO_SHARED_SELECT_CMDS; j++) {
		char dictid_str[64];
		int dictid_len;

		dictid_len = ll2string(dictid_str, sizeof(dictid_str), j);
		shared.select[j] = createObject(OBJ_STRING,
			sdscatprintf(sdsempty(),
				"*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
				dictid_len, dictid_str));
	}
	shared.messagebulk = createStringObject("$7\r\nmessage\r\n", 13);
	shared.pmessagebulk = createStringObject("$8\r\npmessage\r\n", 14);
	shared.subscribebulk = createStringObject("$9\r\nsubscribe\r\n", 15);
	shared.unsubscribebulk = createStringObject("$11\r\nunsubscribe\r\n", 18);
	shared.ssubscribebulk = createStringObject("$10\r\nssubscribe\r\n", 17);
	shared.sunsubscribebulk = createStringObject("$12\r\nsunsubscribe\r\n", 19);
	shared.smessagebulk = createStringObject("$8\r\nsmessage\r\n", 14);
	shared.psubscribebulk = createStringObject("$10\r\npsubscribe\r\n", 17);
	shared.punsubscribebulk = createStringObject("$12\r\npunsubscribe\r\n", 19);

	/* Shared command names */
	shared.del = createStringObject("DEL", 3);
	shared.unlink = createStringObject("UNLINK", 6);
	shared.rpop = createStringObject("RPOP", 4);
	shared.lpop = createStringObject("LPOP", 4);
	shared.lpush = createStringObject("LPUSH", 5);
	shared.rpoplpush = createStringObject("RPOPLPUSH", 9);
	shared.lmove = createStringObject("LMOVE", 5);
	shared.blmove = createStringObject("BLMOVE", 6);
	shared.zpopmin = createStringObject("ZPOPMIN", 7);
	shared.zpopmax = createStringObject("ZPOPMAX", 7);
	shared.multi = createStringObject("MULTI", 5);
	shared.exec = createStringObject("EXEC", 4);
	shared.hset = createStringObject("HSET", 4);
	shared.srem = createStringObject("SREM", 4);
	shared.xgroup = createStringObject("XGROUP", 6);
	shared.xclaim = createStringObject("XCLAIM", 6);
	shared.script = createStringObject("SCRIPT", 6);
	shared.replconf = createStringObject("REPLCONF", 8);
	shared.pexpireat = createStringObject("PEXPIREAT", 9);
	shared.pexpire = createStringObject("PEXPIRE", 7);
	shared.persist = createStringObject("PERSIST", 7);
	shared.set = createStringObject("SET", 3);
	shared.eval = createStringObject("EVAL", 4);

	/* Shared command argument */
	shared.left = createStringObject("left", 4);
	shared.right = createStringObject("right", 5);
	shared.pxat = createStringObject("PXAT", 4);
	shared.time = createStringObject("TIME", 4);
	shared.retrycount = createStringObject("RETRYCOUNT", 10);
	shared.force = createStringObject("FORCE", 5);
	shared.justid = createStringObject("JUSTID", 6);
	shared.entriesread = createStringObject("ENTRIESREAD", 11);
	shared.lastid = createStringObject("LASTID", 6);
	shared.default_username = createStringObject("default", 7);
	shared.ping = createStringObject("ping", 4);
	shared.setid = createStringObject("SETID", 5);
	shared.keepttl = createStringObject("KEEPTTL", 7);
	shared.absttl = createStringObject("ABSTTL", 6);
	shared.load = createStringObject("LOAD", 4);
	shared.createconsumer = createStringObject("CREATECONSUMER", 14);
	shared.getack = createStringObject("GETACK", 6);
	shared.special_asterick = createStringObject("*", 1);
	shared.special_equals = createStringObject("=", 1);
	shared.redacted = makeObjectShared(createStringObject("(redacted)", 10));

	for (j = 0; j < OBJ_SHARED_INTEGERS; j++) {
		shared.integers[j] =
			makeObjectShared(createObject(OBJ_STRING, (void*)(long)j));
		shared.integers[j]->encoding = OBJ_ENCODING_INT;
	}
	for (j = 0; j < OBJ_SHARED_BULKHDR_LEN; j++) {
		shared.mbulkhdr[j] = createObject(OBJ_STRING,
			sdscatprintf(sdsempty(), "*%d\r\n", j));
		shared.bulkhdr[j] = createObject(OBJ_STRING,
			sdscatprintf(sdsempty(), "$%d\r\n", j));
		shared.maphdr[j] = createObject(OBJ_STRING,
			sdscatprintf(sdsempty(), "%%%d\r\n", j));
		shared.sethdr[j] = createObject(OBJ_STRING,
			sdscatprintf(sdsempty(), "~%d\r\n", j));
	}
	/* The following two shared objects, minstring and maxstring, are not
	 * actually used for their value but as a special object meaning
	 * respectively the minimum possible string and the maximum possible
	 * string in string comparisons for the ZRANGEBYLEX command. */
	shared.minstring = sdsnew("minstring");
	shared.maxstring = sdsnew("maxstring");
}

void initServerClientMemUsageBuckets() {
	if (server.client_mem_usage_buckets)
		return;
	server.client_mem_usage_buckets = zmalloc(sizeof(clientMemUsageBucket)*CLIENT_MEM_USAGE_BUCKETS);
	for (int j = 0; j < CLIENT_MEM_USAGE_BUCKETS; j++) {
		server.client_mem_usage_buckets[j].mem_usage_sum = 0;
		server.client_mem_usage_buckets[j].clients = listCreate();
	}
}

void freeServerClientMemUsageBuckets() {
	if (!server.client_mem_usage_buckets)
		return;
	for (int j = 0; j < CLIENT_MEM_USAGE_BUCKETS; j++)
		listRelease(server.client_mem_usage_buckets[j].clients);
	zfree(server.client_mem_usage_buckets);
	server.client_mem_usage_buckets = NULL;
}

void initServerConfig(void) {
	int j;
	char *default_bindaddr[CONFIG_DEFAULT_BINDADDR_COUNT] = CONFIG_DEFAULT_BINDADDR;

	initConfigValues();
	updateCachedTime(1);
	getRandomHexChars(server.runid, CONFIG_RUN_ID_SIZE);
	server.runid[CONFIG_RUN_ID_SIZE] = '\0';
	server.hz = CONFIG_DEFAULT_HZ; /* Initialize it ASAP, even if it may get
					  updated later after loading the config.
					  This value may be used before the server
					  is initialized. */
	server.timezone = getTimeZone(); /* Initialized by tzset(). */
	server.configfile = NULL;
	server.executable = NULL;
	server.arch_bits = (sizeof(long) == 8) ? 64 : 32;
	server.bindaddr_count = CONFIG_DEFAULT_BINDADDR_COUNT;
	for (j = 0; j < CONFIG_DEFAULT_BINDADDR_COUNT; j++)
		server.bindaddr[j] = zstrdup(default_bindaddr[j]);
	server.ipfd.count = 0;
	server.tlsfd.count = 0;
	server.sofd = -1;
	server.active_expire_enabled = 1;
	server.skip_checksum_validation = 0;
	server.loading = 0;
	server.async_loading = 0;
	server.loading_rdb_used_mem = 0;

	server.active_defrag_running = 0;
	server.notify_keyspace_events = 0;
	server.blocked_clients = 0;
	memset(server.blocked_clients_by_type, 0,
		sizeof(server.blocked_clients_by_type));
	server.shutdown_asap = 0;
	server.shutdown_flags = 0;
	server.shutdown_mstime = 0;
	server.next_client_id = 1; /* Client IDs, start from 1 .*/
	server.page_size = sysconf(_SC_PAGESIZE);
	server.pause_cron = 0;

	server.latency_tracking_info_percentiles_len = 3;
	server.latency_tracking_info_percentiles = zmalloc(sizeof(double)*(server.latency_tracking_info_percentiles_len));
	server.latency_tracking_info_percentiles[0] = 50.0;  /* p50 */
	server.latency_tracking_info_percentiles[1] = 99.0;  /* p99 */
	server.latency_tracking_info_percentiles[2] = 99.9;  /* p999 */

	unsigned int lruclock = getLRUClock();
	atomicSet(server.lruclock, lruclock);
	resetServerSaveParams();

	appendServerSaveParams(60 * 60, 1);  /* save after 1 hour and 1 change */
	appendServerSaveParams(300, 100);  /* save after 5 minutes and 100 changes */
	appendServerSaveParams(60, 10000); /* save after 1 minute and 10000 changes */

	/* Client output buffer limits */
	for (j = 0; j < CLIENT_TYPE_OBUF_COUNT; j++)
		server.client_obuf_limits[j] = clientBufferLimitsDefaults[j];

	/* Linux OOM Score config */
	for (j = 0; j < CONFIG_OOM_COUNT; j++)
		server.oom_score_adj_values[j] = configOOMScoreAdjValuesDefaults[j];

	/* Double constants initialization */
	R_Zero = 0.0;
	R_PosInf = 1.0 / R_Zero;
	R_NegInf = -1.0 / R_Zero;
	R_Nan = R_Zero / R_Zero;

	/* Command table -- we initialize it here as it is part of the
	 * initial configuration, since command names may be changed via
	 * redis.conf using the rename-command directive. */
	server.commands = dictCreate(&commandTableDictType);
	server.orig_commands = dictCreate(&commandTableDictType);
	populateCommandTable();

	/* Debugging */
	server.watchdog_period = 0;
}

extern char **environ;

/* Restart the server, executing the same executable that started this
 * instance, with the same arguments and configuration file.
 *
 * The function is designed to directly call execve() so that the new
 * server instance will retain the PID of the previous one.
 *
 * The list of flags, that may be bitwise ORed together, alter the
 * behavior of this function:
 *
 * RESTART_SERVER_NONE              No flags.
 * RESTART_SERVER_GRACEFULLY        Do a proper shutdown before restarting.
 * RESTART_SERVER_CONFIG_REWRITE    Rewrite the config file before restarting.
 *
 * On success the function does not return, because the process turns into
 * a different process. On error C_ERR is returned. */
int restartServer(int flags, mstime_t delay) {
	int j;

	/* Check if we still have accesses to the executable that started this
	 * server instance. */
	if (access(server.executable, X_OK) == -1) {
		serverLog(LL_WARNING, "Can't restart: this process has no "
			"permissions to execute %s", server.executable);
		return C_ERR;
	}

	/* Config rewriting. */
	if (flags & RESTART_SERVER_CONFIG_REWRITE &&
		server.configfile &&
		rewriteConfig(server.configfile, 0) == -1)
	{
		serverLog(LL_WARNING, "Can't restart: configuration rewrite process "
			"failed: %s", strerror(errno));
		return C_ERR;
	}

	/* Perform a proper shutdown. We don't wait for lagging replicas though. */
	if (flags & RESTART_SERVER_GRACEFULLY &&
		prepareForShutdown(SHUTDOWN_NOW) != C_OK)
	{
		serverLog(LL_WARNING, "Can't restart: error preparing for shutdown");
		return C_ERR;
	}

	/* Close all file descriptors, with the exception of stdin, stdout, stderr
	 * which are useful if we restart a Redis server which is not daemonized. */
	for (j = 3; j < (int)server.maxclients + 1024; j++) {
		/* Test the descriptor validity before closing it, otherwise
		 * Valgrind issues a warning on close(). */
		if (fcntl(j, F_GETFD) != -1) close(j);
	}

	/* Execute the server with the original command line. */
	if (delay) usleep(delay * 1000);
	zfree(server.exec_argv[0]);
	server.exec_argv[0] = zstrdup(server.executable);
	execve(server.executable, server.exec_argv, environ);

	/* If an error occurred here, there is nothing we can do, but exit. */
	_exit(1);

	return C_ERR; /* Never reached. */
}

/* This function will configure the current process's oom_score_adj according
 * to user specified configuration. This is currently implemented on Linux
 * only.
 *
 * A process_class value of -1 implies OOM_CONFIG_MASTER or OOM_CONFIG_REPLICA,
 * depending on current role.
 */
int setOOMScoreAdj(int process_class) {
	if (process_class == -1)
		process_class = CONFIG_OOM_MASTER;

	serverAssert(process_class >= 0 && process_class < CONFIG_OOM_COUNT);

#ifdef HAVE_PROC_OOM_SCORE_ADJ
	/* The following statics are used to indicate Redis has changed the process's oom score.
	 * And to save the original score so we can restore it later if needed.
	 * We need this so when we disabled oom-score-adj (also during configuration rollback
	 * when another configuration parameter was invalid and causes a rollback after
	 * applying a new oom-score) we can return to the oom-score value from before our
	 * adjustments. */
	static int oom_score_adjusted_by_redis = 0;
	static int oom_score_adj_base = 0;

	int fd;
	int val;
	char buf[64];

	if (server.oom_score_adj != OOM_SCORE_ADJ_NO) {
		if (!oom_score_adjusted_by_redis) {
			oom_score_adjusted_by_redis = 1;
			/* Backup base value before enabling Redis control over oom score */
			fd = open("/proc/self/oom_score_adj", O_RDONLY);
			if (fd < 0 || read(fd, buf, sizeof(buf)) < 0) {
				serverLog(LL_WARNING, "Unable to read oom_score_adj: %s", strerror(errno));
				if (fd != -1) close(fd);
				return C_ERR;
			}
			oom_score_adj_base = atoi(buf);
			close(fd);
		}

		val = server.oom_score_adj_values[process_class];
		if (server.oom_score_adj == OOM_SCORE_RELATIVE)
			val += oom_score_adj_base;
		if (val > 1000) val = 1000;
		if (val < -1000) val = -1000;
	}
	else if (oom_score_adjusted_by_redis) {
		oom_score_adjusted_by_redis = 0;
		val = oom_score_adj_base;
	}
	else {
		return C_OK;
	}

	snprintf(buf, sizeof(buf) - 1, "%d\n", val);

	fd = open("/proc/self/oom_score_adj", O_WRONLY);
	if (fd < 0 || write(fd, buf, strlen(buf)) < 0) {
		serverLog(LL_WARNING, "Unable to write oom_score_adj: %s", strerror(errno));
		if (fd != -1) close(fd);
		return C_ERR;
	}

	close(fd);
	return C_OK;
#else
	/* Unsupported */
	return C_ERR;
#endif
}

/* This function will try to raise the max number of open files accordingly to
 * the configured max number of clients. It also reserves a number of file
 * descriptors (CONFIG_MIN_RESERVED_FDS) for extra operations of
 * persistence, listening sockets, log files and so forth.
 *
 * If it will not be possible to set the limit accordingly to the configured
 * max number of clients, the function will do the reverse setting
 * server.maxclients to the value that we can actually handle. */
void adjustOpenFilesLimit(void) {
	rlim_t maxfiles = server.maxclients + CONFIG_MIN_RESERVED_FDS;
	struct rlimit limit;

	if (getrlimit(RLIMIT_NOFILE, &limit) == -1) {
		serverLog(LL_WARNING, "Unable to obtain the current NOFILE limit (%s), assuming 1024 and setting the max clients configuration accordingly.",
			strerror(errno));
		server.maxclients = 1024 - CONFIG_MIN_RESERVED_FDS;
	}
	else {
		rlim_t oldlimit = limit.rlim_cur;

		/* Set the max number of files if the current limit is not enough
		 * for our needs. */
		if (oldlimit < maxfiles) {
			rlim_t bestlimit;
			int setrlimit_error = 0;

			/* Try to set the file limit to match 'maxfiles' or at least
			 * to the higher value supported less than maxfiles. */
			bestlimit = maxfiles;
			while (bestlimit > oldlimit) {
				rlim_t decr_step = 16;

				limit.rlim_cur = bestlimit;
				limit.rlim_max = bestlimit;
				if (setrlimit(RLIMIT_NOFILE, &limit) != -1) break;
				setrlimit_error = errno;

				/* We failed to set file limit to 'bestlimit'. Try with a
				 * smaller limit decrementing by a few FDs per iteration. */
				if (bestlimit < decr_step) {
					bestlimit = oldlimit;
					break;
				}
				bestlimit -= decr_step;
			}

			/* Assume that the limit we get initially is still valid if
			 * our last try was even lower. */
			if (bestlimit < oldlimit) bestlimit = oldlimit;

			if (bestlimit < maxfiles) {
				unsigned int old_maxclients = server.maxclients;
				server.maxclients = bestlimit - CONFIG_MIN_RESERVED_FDS;
				/* maxclients is unsigned so may overflow: in order
				 * to check if maxclients is now logically less than 1
				 * we test indirectly via bestlimit. */
				if (bestlimit <= CONFIG_MIN_RESERVED_FDS) {
					serverLog(LL_WARNING, "Your current 'ulimit -n' "
						"of %llu is not enough for the server to start. "
						"Please increase your open file limit to at least "
						"%llu. Exiting.",
						(unsigned long long) oldlimit,
						(unsigned long long) maxfiles);
					exit(1);
				}
				serverLog(LL_WARNING, "You requested maxclients of %d "
					"requiring at least %llu max file descriptors.",
					old_maxclients,
					(unsigned long long) maxfiles);
				serverLog(LL_WARNING, "Server can't set maximum open files "
					"to %llu because of OS error: %s.",
					(unsigned long long) maxfiles, strerror(setrlimit_error));
				serverLog(LL_WARNING, "Current maximum open files is %llu. "
					"maxclients has been reduced to %d to compensate for "
					"low ulimit. "
					"If you need higher maxclients increase 'ulimit -n'.",
					(unsigned long long) bestlimit, server.maxclients);
			}
			else {
				serverLog(LL_NOTICE, "Increased maximum number of open files "
					"to %llu (it was originally set to %llu).",
					(unsigned long long) maxfiles,
					(unsigned long long) oldlimit);
			}
		}
	}
}

/* Check that server.tcp_backlog can be actually enforced in Linux according
 * to the value of /proc/sys/net/core/somaxconn, or warn about it. */
void checkTcpBacklogSettings(void) {
#if defined(HAVE_PROC_SOMAXCONN)
	FILE *fp = fopen("/proc/sys/net/core/somaxconn", "r");
	char buf[1024];
	if (!fp) return;
	if (fgets(buf, sizeof(buf), fp) != NULL) {
		int somaxconn = atoi(buf);
		if (somaxconn > 0 && somaxconn < server.tcp_backlog) {
			serverLog(LL_WARNING, "WARNING: The TCP backlog setting of %d cannot be enforced because /proc/sys/net/core/somaxconn is set to the lower value of %d.", server.tcp_backlog, somaxconn);
		}
	}
	fclose(fp);
#elif defined(HAVE_SYSCTL_KIPC_SOMAXCONN)
	int somaxconn, mib[3];
	size_t len = sizeof(int);

	mib[0] = CTL_KERN;
	mib[1] = KERN_IPC;
	mib[2] = KIPC_SOMAXCONN;

	if (sysctl(mib, 3, &somaxconn, &len, NULL, 0) == 0) {
		if (somaxconn > 0 && somaxconn < server.tcp_backlog) {
			serverLog(LL_WARNING, "WARNING: The TCP backlog setting of %d cannot be enforced because kern.ipc.somaxconn is set to the lower value of %d.", server.tcp_backlog, somaxconn);
		}
	}
#elif defined(HAVE_SYSCTL_KERN_SOMAXCONN)
	int somaxconn, mib[2];
	size_t len = sizeof(int);

	mib[0] = CTL_KERN;
	mib[1] = KERN_SOMAXCONN;

	if (sysctl(mib, 2, &somaxconn, &len, NULL, 0) == 0) {
		if (somaxconn > 0 && somaxconn < server.tcp_backlog) {
			serverLog(LL_WARNING, "WARNING: The TCP backlog setting of %d cannot be enforced because kern.somaxconn is set to the lower value of %d.", server.tcp_backlog, somaxconn);
		}
	}
#elif defined(SOMAXCONN)
	if (SOMAXCONN < server.tcp_backlog) {
		serverLog(LL_WARNING, "WARNING: The TCP backlog setting of %d cannot be enforced because SOMAXCONN is set to the lower value of %d.", server.tcp_backlog, SOMAXCONN);
	}
#endif
}

void closeSocketListeners(socketFds *sfd) {
	int j;

	for (j = 0; j < sfd->count; j++) {
		if (sfd->fd[j] == -1) continue;

		aeDeleteFileEvent(server.el, sfd->fd[j], AE_READABLE);
		close(sfd->fd[j]);
	}

	sfd->count = 0;
}

/* Create an event handler for accepting new connections in TCP or TLS domain sockets.
 * This works atomically for all socket fds */
int createSocketAcceptHandler(socketFds *sfd, aeFileProc *accept_handler) {
	int j;

	for (j = 0; j < sfd->count; j++) {
		if (aeCreateFileEvent(server.el, sfd->fd[j], AE_READABLE, accept_handler, NULL) == AE_ERR) {
			/* Rollback */
			for (j = j - 1; j >= 0; j--) aeDeleteFileEvent(server.el, sfd->fd[j], AE_READABLE);
			return C_ERR;
		}
	}
	return C_OK;
}

/* Initialize a set of file descriptors to listen to the specified 'port'
 * binding the addresses specified in the Redis server configuration.
 *
 * The listening file descriptors are stored in the integer array 'fds'
 * and their number is set in '*count'.
 *
 * The addresses to bind are specified in the global server.bindaddr array
 * and their number is server.bindaddr_count. If the server configuration
 * contains no specific addresses to bind, this function will try to
 * bind * (all addresses) for both the IPv4 and IPv6 protocols.
 *
 * On success the function returns C_OK.
 *
 * On error the function returns C_ERR. For the function to be on
 * error, at least one of the server.bindaddr addresses was
 * impossible to bind, or no bind addresses were specified in the server
 * configuration but the function is not able to bind * for at least
 * one of the IPv4 or IPv6 protocols. */
int listenToPort(int port, socketFds *sfd) {
	int j;
	char **bindaddr = server.bindaddr;

	/* If we have no bind address, we don't listen on a TCP socket */
	if (server.bindaddr_count == 0) return C_OK;

	for (j = 0; j < server.bindaddr_count; j++) {
		char* addr = bindaddr[j];
		int optional = *addr == '-';
		if (optional) addr++;
		if (strchr(addr, ':')) {
			/* Bind IPv6 address. */
			sfd->fd[sfd->count] = anetTcp6Server(server.neterr, port, addr, server.tcp_backlog);
		}
		else {
			/* Bind IPv4 address. */
			sfd->fd[sfd->count] = anetTcpServer(server.neterr, port, addr, server.tcp_backlog);
		}
		if (sfd->fd[sfd->count] == ANET_ERR) {
			int net_errno = errno;
			serverLog(LL_WARNING,
				"Warning: Could not create server TCP listening socket %s:%d: %s",
				addr, port, server.neterr);
			if (net_errno == EADDRNOTAVAIL && optional)
				continue;
			if (net_errno == ENOPROTOOPT || net_errno == EPROTONOSUPPORT ||
				net_errno == ESOCKTNOSUPPORT || net_errno == EPFNOSUPPORT ||
				net_errno == EAFNOSUPPORT)
				continue;

			/* Rollback successful listens before exiting */
			closeSocketListeners(sfd);
			return C_ERR;
		}
		if (server.socket_mark_id > 0) anetSetSockMarkId(NULL, sfd->fd[sfd->count], server.socket_mark_id);
		anetNonBlock(NULL, sfd->fd[sfd->count]);
		anetCloexec(sfd->fd[sfd->count]);
		sfd->count++;
	}
	return C_OK;
}

/* Resets the stats that we expose via INFO or other means that we want
 * to reset via CONFIG RESETSTAT. The function is also used in order to
 * initialize these fields in initServer() at server startup. */
void resetServerStats(void) {
	int j;

	server.stat_numcommands = 0;
	server.stat_numconnections = 0;
	server.stat_expiredkeys = 0;
	server.stat_expired_stale_perc = 0;
	server.stat_expired_time_cap_reached_count = 0;
	server.stat_expire_cycle_time_used = 0;
	server.stat_evictedkeys = 0;
	server.stat_evictedclients = 0;
	server.stat_total_eviction_exceeded_time = 0;
	server.stat_last_eviction_exceeded_time = 0;
	server.stat_keyspace_misses = 0;
	server.stat_keyspace_hits = 0;
	server.stat_active_defrag_hits = 0;
	server.stat_active_defrag_misses = 0;
	server.stat_active_defrag_key_hits = 0;
	server.stat_active_defrag_key_misses = 0;
	server.stat_active_defrag_scanned = 0;
	server.stat_total_active_defrag_time = 0;
	server.stat_last_active_defrag_time = 0;
	server.stat_fork_time = 0;
	server.stat_fork_rate = 0;
	server.stat_total_forks = 0;
	server.stat_rejected_conn = 0;
	server.stat_sync_full = 0;
	server.stat_sync_partial_ok = 0;
	server.stat_sync_partial_err = 0;
	server.stat_io_reads_processed = 0;
	atomicSet(server.stat_total_reads_processed, 0);
	server.stat_io_writes_processed = 0;
	atomicSet(server.stat_total_writes_processed, 0);
	for (j = 0; j < STATS_METRIC_COUNT; j++) {
		server.inst_metric[j].idx = 0;
		server.inst_metric[j].last_sample_time = mstime();
		server.inst_metric[j].last_sample_count = 0;
		memset(server.inst_metric[j].samples, 0,
			sizeof(server.inst_metric[j].samples));
	}
	server.stat_aof_rewrites = 0;
	server.stat_rdb_saves = 0;
	server.stat_aofrw_consecutive_failures = 0;
	atomicSet(server.stat_net_input_bytes, 0);
	atomicSet(server.stat_net_output_bytes, 0);
	atomicSet(server.stat_net_repl_input_bytes, 0);
	atomicSet(server.stat_net_repl_output_bytes, 0);
	server.stat_unexpected_error_replies = 0;
	server.stat_total_error_replies = 0;
	server.stat_dump_payload_sanitizations = 0;
	server.stat_reply_buffer_shrinks = 0;
	server.stat_reply_buffer_expands = 0;
}

/* Make the thread killable at any time, so that kill threads functions
 * can work reliably (default cancelability type is PTHREAD_CANCEL_DEFERRED).
 * Needed for pthread_cancel used by the fast memory test used by the crash report. */
void makeThreadKillable(void) {
	pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
	pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
}

void initServer(void) {
	int j;

	signal(SIGHUP, SIG_IGN);
	signal(SIGPIPE, SIG_IGN);
	setupSignalHandlers();
	makeThreadKillable();

	if (server.syslog_enabled) {
		openlog(server.syslog_ident, LOG_PID | LOG_NDELAY | LOG_NOWAIT,
			server.syslog_facility);
	}

	/* Initialization after setting defaults from the config system. */
	server.hz = server.config_hz;
	server.pid = getpid();
	server.in_fork_child = CHILD_TYPE_NONE;
	server.main_thread_id = pthread_self();
	server.current_client = NULL;
	server.errors = raxNew();
	server.fixed_time_expire = 0;
	server.in_nested_call = 0;
	server.clients = listCreate();
	server.clients_index = raxNew();
	server.clients_to_close = listCreate();
	server.slaves = listCreate();
	server.monitors = listCreate();
	server.clients_pending_write = listCreate();
	server.clients_pending_read = listCreate();
	server.clients_timeout_table = raxNew();
	server.replication_allowed = 1;
	server.unblocked_clients = listCreate();
	server.ready_keys = listCreate();
	server.tracking_pending_keys = listCreate();
	server.client_pause_type = CLIENT_PAUSE_OFF;
	server.client_pause_end_time = 0;
	memset(server.client_pause_per_purpose, 0,
		sizeof(server.client_pause_per_purpose));
	server.postponed_clients = listCreate();
	server.events_processed_while_blocked = 0;
	server.system_memory_size = zmalloc_get_memory_size();
	server.blocked_last_cron = 0;
	server.blocking_op_nesting = 0;
	server.thp_enabled = 0;
	server.reply_buffer_peak_reset_time = REPLY_BUFFER_DEFAULT_PEAK_RESET_TIME;
	server.reply_buffer_resizing_enabled = 1;
	server.client_mem_usage_buckets = NULL;

	if ((server.tls_port || server.tls_replication || server.tls_cluster)
		&& tlsConfigure(&server.tls_ctx_config) == C_ERR) {
		serverLog(LL_WARNING, "Failed to configure TLS. Check logs for more info.");
		exit(1);
	}

	createSharedObjects();
	adjustOpenFilesLimit();
	const char *clk_msg = monotonicInit();
	serverLog(LL_NOTICE, "monotonic clock: %s", clk_msg);
	server.el = aeCreateEventLoop(server.maxclients + CONFIG_FDSET_INCR);
	if (server.el == NULL) {
		serverLog(LL_WARNING,
			"Failed creating the event loop. Error message: '%s'",
			strerror(errno));
		exit(1);
	}
	/* Open the TCP listening socket for the user commands. */
	if (server.port != 0 &&
		listenToPort(server.port, &server.ipfd) == C_ERR) {
		/* Note: the following log text is matched by the test suite. */
		serverLog(LL_WARNING, "Failed listening on port %u (TCP), aborting.", server.port);
		exit(1);
	}
	if (server.tls_port != 0 &&
		listenToPort(server.tls_port, &server.tlsfd) == C_ERR) {
		/* Note: the following log text is matched by the test suite. */
		serverLog(LL_WARNING, "Failed listening on port %u (TLS), aborting.", server.tls_port);
		exit(1);
	}

	/* Open the listening Unix domain socket. */
	if (server.unixsocket != NULL) {
		unlink(server.unixsocket); /* don't care if this fails */
		server.sofd = anetUnixServer(server.neterr, server.unixsocket,
			(mode_t)server.unixsocketperm, server.tcp_backlog);
		if (server.sofd == ANET_ERR) {
			serverLog(LL_WARNING, "Failed opening Unix socket: %s", server.neterr);
			exit(1);
		}
		anetNonBlock(NULL, server.sofd);
		anetCloexec(server.sofd);
	}

	/* Abort if there are no listening sockets at all. */
	if (server.ipfd.count == 0 && server.tlsfd.count == 0 && server.sofd < 0) {
		serverLog(LL_WARNING, "Configured to not listen anywhere, exiting.");
		exit(1);
	}

	server.cronloops = 0;
	server.in_exec = 0;
	server.busy_module_yield_flags = BUSY_MODULE_YIELD_NONE;
	server.busy_module_yield_reply = NULL;
	server.core_propagates = 0;
	server.propagate_no_multi = 0;
	server.module_ctx_nesting = 0;
	server.client_pause_in_transaction = 0;
	server.child_pid = -1;
	server.child_type = CHILD_TYPE_NONE;
	resetServerStats();
	/* A few stats we don't want to reset: server startup time, and peak mem. */
	server.stat_starttime = time(NULL);
	server.stat_peak_memory = 0;
	server.stat_current_cow_peak = 0;
	server.stat_current_cow_bytes = 0;
	server.stat_current_cow_updated = 0;
	server.stat_current_save_keys_processed = 0;
	server.stat_current_save_keys_total = 0;
	server.stat_rdb_cow_bytes = 0;
	server.stat_aof_cow_bytes = 0;
	server.stat_module_cow_bytes = 0;
	server.stat_module_progress = 0;
	for (int j = 0; j < CLIENT_TYPE_COUNT; j++)
		server.stat_clients_type_memory[j] = 0;
	server.stat_cluster_links_memory = 0;
	server.cron_malloc_stats.zmalloc_used = 0;
	server.cron_malloc_stats.process_rss = 0;
	server.cron_malloc_stats.allocator_allocated = 0;
	server.cron_malloc_stats.allocator_active = 0;
	server.cron_malloc_stats.allocator_resident = 0;

	/* Create the timer callback, this is our way to process many background
	 * operations incrementally, like clients timeout, eviction of unaccessed
	 * expired keys and so forth. */
	if (aeCreateTimeEvent(server.el, 1, serverCron, NULL, NULL) == AE_ERR) {
		serverPanic("Can't create event loop timers.");
		exit(1);
	}

	/* Create an event handler for accepting new connections in TCP and Unix
	 * domain sockets. */
	if (createSocketAcceptHandler(&server.ipfd, acceptTcpHandler) != C_OK) {
		serverPanic("Unrecoverable error creating TCP socket accept handler.");
	}
	if (createSocketAcceptHandler(&server.tlsfd, acceptTLSHandler) != C_OK) {
		serverPanic("Unrecoverable error creating TLS socket accept handler.");
	}
	if (server.sofd > 0 && aeCreateFileEvent(server.el, server.sofd, AE_READABLE,
		acceptUnixHandler, NULL) == AE_ERR) serverPanic("Unrecoverable error creating server.sofd file event.");

	/* Register before and after sleep handlers (note this needs to be done
	 * before loading persistence since it is used by processEventsWhileBlocked. */
	aeSetBeforeSleepProc(server.el, beforeSleep);
	aeSetAfterSleepProc(server.el, afterSleep);

	/* 32 bit instances are limited to 4GB of address space, so if there is
	 * no explicit limit in the user provided configuration we set a limit
	 * at 3 GB using maxmemory with 'noeviction' policy'. This avoids
	 * useless crashes of the Redis instance for out of memory. */
	if (server.arch_bits == 32 && server.maxmemory == 0) {
		serverLog(LL_WARNING, "Warning: 32 bit instance detected but no memory limit set. Setting 3 GB maxmemory limit with 'noeviction' policy now.");
		server.maxmemory = 3072LL * (1024 * 1024); /* 3 GB */
		server.maxmemory_policy = MAXMEMORY_NO_EVICTION;
	}

	slowlogInit();
	latencyMonitorInit();

	/* Initialize ACL default password if it exists */
	ACLUpdateDefaultUserPassword(server.requirepass);

	applyWatchdogPeriod();

	if (server.maxmemory_clients != 0)
		initServerClientMemUsageBuckets();
}

/* Some steps in server initialization need to be done last (after modules
 * are loaded).
 * Specifically, creation of threads due to a race bug in ld.so, in which
 * Thread Local Storage initialization collides with dlopen call.
 * see: https://sourceware.org/bugzilla/show_bug.cgi?id=19329 */
void InitServerLast() {
	initThreadedIO();
	set_jemalloc_bg_thread(server.jemalloc_bg_thread);
	server.initial_memory_usage = zmalloc_used_memory();
}

/* The purpose of this function is to try to "glue" consecutive range
 * key specs in order to build the legacy (first,last,step) spec
 * used by the COMMAND command.
 * By far the most common case is just one range spec (e.g. SET)
 * but some commands' ranges were split into two or more ranges
 * in order to have different flags for different keys (e.g. SMOVE,
 * first key is "RW ACCESS DELETE", second key is "RW INSERT").
 *
 * Additionally set the CMD_MOVABLE_KEYS flag for commands that may have key
 * names in their arguments, but the legacy range spec doesn't cover all of them.
 *
 * This function uses very basic heuristics and is "best effort":
 * 1. Only commands which have only "range" specs are considered.
 * 2. Only range specs with keystep of 1 are considered.
 * 3. The order of the range specs must be ascending (i.e.
 *    lastkey of spec[i] == firstkey-1 of spec[i+1]).
 *
 * This function will succeed on all native Redis commands and may
 * fail on module commands, even if it only has "range" specs that
 * could actually be "glued", in the following cases:
 * 1. The order of "range" specs is not ascending (e.g. the spec for
 *    the key at index 2 was added before the spec of the key at
 *    index 1).
 * 2. The "range" specs have keystep >1.
 *
 * If this functions fails it means that the legacy (first,last,step)
 * spec used by COMMAND will show 0,0,0. This is not a dire situation
 * because anyway the legacy (first,last,step) spec is to be deprecated
 * and one should use the new key specs scheme.
 */
void populateCommandLegacyRangeSpec(struct redisCommand *c) {
	memset(&c->legacy_range_key_spec, 0, sizeof(c->legacy_range_key_spec));

	/* Set the movablekeys flag if we have a GETKEYS flag for modules.
	 * Note that for native redis commands, we always have keyspecs,
	 * with enough information to rely on for movablekeys. */
	if (c->flags & CMD_MODULE_GETKEYS)
		c->flags |= CMD_MOVABLE_KEYS;

	/* no key-specs, no keys, exit. */
	if (c->key_specs_num == 0) {
		return;
	}

	if (c->key_specs_num == 1 &&
		c->key_specs[0].begin_search_type == KSPEC_BS_INDEX &&
		c->key_specs[0].find_keys_type == KSPEC_FK_RANGE)
	{
		/* Quick win, exactly one range spec. */
		c->legacy_range_key_spec = c->key_specs[0];
		/* If it has the incomplete flag, set the movablekeys flag on the command. */
		if (c->key_specs[0].flags & CMD_KEY_INCOMPLETE)
			c->flags |= CMD_MOVABLE_KEYS;
		return;
	}

	int firstkey = INT_MAX, lastkey = 0;
	int prev_lastkey = 0;
	for (int i = 0; i < c->key_specs_num; i++) {
		if (c->key_specs[i].begin_search_type != KSPEC_BS_INDEX ||
			c->key_specs[i].find_keys_type != KSPEC_FK_RANGE)
		{
			/* Found an incompatible (non range) spec, skip it, and set the movablekeys flag. */
			c->flags |= CMD_MOVABLE_KEYS;
			continue;
		}
		if (c->key_specs[i].fk.range.keystep != 1 ||
			(prev_lastkey && prev_lastkey != c->key_specs[i].bs.index.pos - 1))
		{
			/* Found a range spec that's not plain (step of 1) or not consecutive to the previous one.
			 * Skip it, and we set the movablekeys flag. */
			c->flags |= CMD_MOVABLE_KEYS;
			continue;
		}
		if (c->key_specs[i].flags & CMD_KEY_INCOMPLETE) {
			/* The spec we're using is incomplete, we can use it, but we also have to set the movablekeys flag. */
			c->flags |= CMD_MOVABLE_KEYS;
		}
		firstkey = min(firstkey, c->key_specs[i].bs.index.pos);
		/* Get the absolute index for lastkey (in the "range" spec, lastkey is relative to firstkey) */
		int lastkey_abs_index = c->key_specs[i].fk.range.lastkey;
		if (lastkey_abs_index >= 0)
			lastkey_abs_index += c->key_specs[i].bs.index.pos;
		/* For lastkey we use unsigned comparison to handle negative values correctly */
		lastkey = max((unsigned)lastkey, (unsigned)lastkey_abs_index);
		prev_lastkey = lastkey;
	}

	if (firstkey == INT_MAX) {
		/* Couldn't find range specs, the legacy range spec will remain empty, and we set the movablekeys flag. */
		c->flags |= CMD_MOVABLE_KEYS;
		return;
	}

	serverAssert(firstkey != 0);
	serverAssert(lastkey != 0);

	c->legacy_range_key_spec.begin_search_type = KSPEC_BS_INDEX;
	c->legacy_range_key_spec.bs.index.pos = firstkey;
	c->legacy_range_key_spec.find_keys_type = KSPEC_FK_RANGE;
	c->legacy_range_key_spec.fk.range.lastkey = lastkey < 0 ? lastkey : (lastkey - firstkey); /* in the "range" spec, lastkey is relative to firstkey */
	c->legacy_range_key_spec.fk.range.keystep = 1;
	c->legacy_range_key_spec.fk.range.limit = 0;
}

sds catSubCommandFullname(const char *parent_name, const char *sub_name) {
	return sdscatfmt(sdsempty(), "%s|%s", parent_name, sub_name);
}

void commandAddSubcommand(struct redisCommand *parent, struct redisCommand *subcommand, const char *declared_name) {
	if (!parent->subcommands_dict)
		parent->subcommands_dict = dictCreate(&commandTableDictType);

	subcommand->parent = parent; /* Assign the parent command */
	subcommand->id = ACLGetCommandID(subcommand->fullname); /* Assign the ID used for ACL. */

	serverAssert(dictAdd(parent->subcommands_dict, sdsnew(declared_name), subcommand) == DICT_OK);
}

/* Set implicit ACl categories (see comment above the definition of
 * struct redisCommand). */
void setImplicitACLCategories(struct redisCommand *c) {
	if (c->flags & CMD_WRITE)
		c->acl_categories |= ACL_CATEGORY_WRITE;
	/* Exclude scripting commands from the RO category. */
	if (c->flags & CMD_READONLY && !(c->acl_categories & ACL_CATEGORY_SCRIPTING))
		c->acl_categories |= ACL_CATEGORY_READ;
	if (c->flags & CMD_ADMIN)
		c->acl_categories |= ACL_CATEGORY_ADMIN | ACL_CATEGORY_DANGEROUS;
	if (c->flags & CMD_PUBSUB)
		c->acl_categories |= ACL_CATEGORY_PUBSUB;
	if (c->flags & CMD_FAST)
		c->acl_categories |= ACL_CATEGORY_FAST;
	if (c->flags & CMD_BLOCKING)
		c->acl_categories |= ACL_CATEGORY_BLOCKING;

	/* If it's not @fast is @slow in this binary world. */
	if (!(c->acl_categories & ACL_CATEGORY_FAST))
		c->acl_categories |= ACL_CATEGORY_SLOW;
}

/* Recursively populate the args structure (setting num_args to the number of
 * subargs) and return the number of args. */
int populateArgsStructure(struct redisCommandArg *args) {
	if (!args)
		return 0;
	int count = 0;
	while (args->name) {
		serverAssert(count < INT_MAX);
		args->num_args = populateArgsStructure(args->subargs);
		count++;
		args++;
	}
	return count;
}

/* Recursively populate the command structure.
 *
 * On success, the function return C_OK. Otherwise C_ERR is returned and we won't
 * add this command in the commands dict. */
int populateCommandStructure(struct redisCommand *c) {
	/* If the command marks with CMD_SENTINEL, it exists in sentinel. */
	if (!(c->flags & CMD_SENTINEL) && server.sentinel_mode)
		return C_ERR;

	/* If the command marks with CMD_ONLY_SENTINEL, it only exists in sentinel. */
	if (c->flags & CMD_ONLY_SENTINEL && !server.sentinel_mode)
		return C_ERR;

	/* Translate the command string flags description into an actual
	 * set of flags. */
	setImplicitACLCategories(c);

	/* Redis commands don't need more args than STATIC_KEY_SPECS_NUM (Number of keys
	 * specs can be greater than STATIC_KEY_SPECS_NUM only for module commands) */
	c->key_specs = c->key_specs_static;
	c->key_specs_max = STATIC_KEY_SPECS_NUM;

	for (int i = 0; i < STATIC_KEY_SPECS_NUM; i++) {
		if (c->key_specs[i].begin_search_type == KSPEC_BS_INVALID)
			break;
		c->key_specs_num++;
	}

	/* Count things so we don't have to use deferred reply in COMMAND reply. */
	while (c->history && c->history[c->num_history].since)
		c->num_history++;
	while (c->tips && c->tips[c->num_tips])
		c->num_tips++;
	c->num_args = populateArgsStructure(c->args);

	/* Handle the legacy range spec and the "movablekeys" flag (must be done after populating all key specs). */
	populateCommandLegacyRangeSpec(c);

	/* Assign the ID used for ACL. */
	c->id = ACLGetCommandID(c->fullname);

	/* Handle subcommands */
	if (c->subcommands) {
		for (int j = 0; c->subcommands[j].declared_name; j++) {
			struct redisCommand *sub = c->subcommands + j;

			sub->fullname = catSubCommandFullname(c->declared_name, sub->declared_name);
			if (populateCommandStructure(sub) == C_ERR)
				continue;

			commandAddSubcommand(c, sub, sub->declared_name);
		}
	}

	return C_OK;
}

extern struct redisCommand redisCommandTable[];

/* Populates the Redis Command Table dict from the static table in commands.c
 * which is auto generated from the json files in the commands folder. */
void populateCommandTable(void) {
	int j;
	struct redisCommand *c;

	for (j = 0;; j++) {
		c = redisCommandTable + j;
		if (c->declared_name == NULL)
			break;

		int retval1, retval2;

		c->fullname = sdsnew(c->declared_name);
		if (populateCommandStructure(c) == C_ERR)
			continue;

		retval1 = dictAdd(server.commands, sdsdup(c->fullname), c);
		/* Populate an additional dictionary that will be unaffected
		 * by rename-command statements in redis.conf. */
		retval2 = dictAdd(server.orig_commands, sdsdup(c->fullname), c);
		serverAssert(retval1 == DICT_OK && retval2 == DICT_OK);
	}
}

void resetCommandTableStats(dict* commands) {
	struct redisCommand *c;
	dictEntry *de;
	dictIterator *di;

	di = dictGetSafeIterator(commands);
	while ((de = dictNext(di)) != NULL) {
		c = (struct redisCommand *) dictGetVal(de);
		c->microseconds = 0;
		c->calls = 0;
		c->rejected_calls = 0;
		c->failed_calls = 0;
		if (c->subcommands_dict)
			resetCommandTableStats(c->subcommands_dict);
	}
	dictReleaseIterator(di);
}

void resetErrorTableStats(void) {
	raxFreeWithCallback(server.errors, zfree);
	server.errors = raxNew();
}

/* ========================== Redis OP Array API ============================ */

void redisOpArrayInit(redisOpArray *oa) {
	oa->ops = NULL;
	oa->numops = 0;
	oa->capacity = 0;
}

int redisOpArrayAppend(redisOpArray *oa, int dbid, robj **argv, int argc, int target) {
	redisOp *op;
	int prev_capacity = oa->capacity;

	if (oa->numops == 0) {
		oa->capacity = 16;
	}
	else if (oa->numops >= oa->capacity) {
		oa->capacity *= 2;
	}

	if (prev_capacity != oa->capacity)
		oa->ops = zrealloc(oa->ops, sizeof(redisOp)*oa->capacity);
	op = oa->ops + oa->numops;
	op->dbid = dbid;
	op->argv = argv;
	op->argc = argc;
	op->target = target;
	oa->numops++;
	return oa->numops;
}

void redisOpArrayFree(redisOpArray *oa) {
	while (oa->numops) {
		int j;
		redisOp *op;

		oa->numops--;
		op = oa->ops + oa->numops;
		for (j = 0; j < op->argc; j++)
			decrRefCount(op->argv[j]);
		zfree(op->argv);
	}
	zfree(oa->ops);
	redisOpArrayInit(oa);
}

/* ====================== Commands lookup and execution ===================== */

int isContainerCommandBySds(sds s) {
	struct redisCommand *base_cmd = dictFetchValue(server.commands, s);
	int has_subcommands = base_cmd && base_cmd->subcommands_dict;
	return has_subcommands;
}

struct redisCommand *lookupSubcommand(struct redisCommand *container, sds sub_name) {
	return dictFetchValue(container->subcommands_dict, sub_name);
}

/* Look up a command by argv and argc
 *
 * If `strict` is not 0 we expect argc to be exact (i.e. argc==2
 * for a subcommand and argc==1 for a top-level command)
 * `strict` should be used every time we want to look up a command
 * name (e.g. in COMMAND INFO) rather than to find the command
 * a user requested to execute (in processCommand).
 */
struct redisCommand *lookupCommandLogic(dict *commands, robj **argv, int argc, int strict) {
	struct redisCommand *base_cmd = dictFetchValue(commands, argv[0]->ptr);
	int has_subcommands = base_cmd && base_cmd->subcommands_dict;
	if (argc == 1 || !has_subcommands) {
		if (strict && argc != 1)
			return NULL;
		/* Note: It is possible that base_cmd->proc==NULL (e.g. CONFIG) */
		return base_cmd;
	}
	else { /* argc > 1 && has_subcommands */
		if (strict && argc != 2)
			return NULL;
		/* Note: Currently we support just one level of subcommands */
		return lookupSubcommand(base_cmd, argv[1]->ptr);
	}
}

struct redisCommand *lookupCommand(robj **argv, int argc) {
	return lookupCommandLogic(server.commands, argv, argc, 0);
}

struct redisCommand *lookupCommandBySdsLogic(dict *commands, sds s) {
	int argc, j;
	sds *strings = sdssplitlen(s, sdslen(s), "|", 1, &argc);
	if (strings == NULL)
		return NULL;
	if (argc > 2) {
		/* Currently we support just one level of subcommands */
		sdsfreesplitres(strings, argc);
		return NULL;
	}

	robj objects[argc];
	robj *argv[argc];
	for (j = 0; j < argc; j++) {
		initStaticStringObject(objects[j], strings[j]);
		argv[j] = &objects[j];
	}

	struct redisCommand *cmd = lookupCommandLogic(commands, argv, argc, 1);
	sdsfreesplitres(strings, argc);
	return cmd;
}

struct redisCommand *lookupCommandBySds(sds s) {
	return lookupCommandBySdsLogic(server.commands, s);
}

struct redisCommand *lookupCommandByCStringLogic(dict *commands, const char *s) {
	struct redisCommand *cmd;
	sds name = sdsnew(s);

	cmd = lookupCommandBySdsLogic(commands, name);
	sdsfree(name);
	return cmd;
}

struct redisCommand *lookupCommandByCString(const char *s) {
	return lookupCommandByCStringLogic(server.commands, s);
}

/* Lookup the command in the current table, if not found also check in
 * the original table containing the original command names unaffected by
 * redis.conf rename-command statement.
 *
 * This is used by functions rewriting the argument vector such as
 * rewriteClientCommandVector() in order to set client->cmd pointer
 * correctly even if the command was renamed. */
struct redisCommand *lookupCommandOrOriginal(robj **argv, int argc) {
	struct redisCommand *cmd = lookupCommandLogic(server.commands, argv, argc, 0);

	if (!cmd) cmd = lookupCommandLogic(server.orig_commands, argv, argc, 0);
	return cmd;
}

/* It is possible to call the function forceCommandPropagation() inside a
 * Redis command implementation in order to to force the propagation of a
 * specific command execution into AOF / Replication. */
void forceCommandPropagation(client *c, int flags) {
	serverAssert(c->cmd->flags & (CMD_WRITE | CMD_MAY_REPLICATE));
	if (flags & PROPAGATE_REPL) c->flags |= CLIENT_FORCE_REPL;
	if (flags & PROPAGATE_AOF) c->flags |= CLIENT_FORCE_AOF;
}

/* Avoid that the executed command is propagated at all. This way we
 * are free to just propagate what we want using the alsoPropagate()
 * API. */
void preventCommandPropagation(client *c) {
	c->flags |= CLIENT_PREVENT_PROP;
}

/* AOF specific version of preventCommandPropagation(). */
void preventCommandAOF(client *c) {
	c->flags |= CLIENT_PREVENT_AOF_PROP;
}

/* Replication specific version of preventCommandPropagation(). */
void preventCommandReplication(client *c) {
	c->flags |= CLIENT_PREVENT_REPL_PROP;
}

/* Log the last command a client executed into the slowlog. */
void slowlogPushCurrentCommand(client *c, struct redisCommand *cmd, ustime_t duration) {
	/* Some commands may contain sensitive data that should not be available in the slowlog. */
	if (cmd->flags & CMD_SKIP_SLOWLOG)
		return;

	/* If command argument vector was rewritten, use the original
	 * arguments. */
	robj **argv = c->original_argv ? c->original_argv : c->argv;
	int argc = c->original_argv ? c->original_argc : c->argc;
	slowlogPushEntryIfNeeded(c, argv, argc, duration);
}

/* Increment the command failure counters (either rejected_calls or failed_calls).
 * The decision which counter to increment is done using the flags argument, options are:
 * * ERROR_COMMAND_REJECTED - update rejected_calls
 * * ERROR_COMMAND_FAILED - update failed_calls
 *
 * The function also reset the prev_err_count to make sure we will not count the same error
 * twice, its possible to pass a NULL cmd value to indicate that the error was counted elsewhere.
 *
 * The function returns true if stats was updated and false if not. */
int incrCommandStatsOnError(struct redisCommand *cmd, int flags) {
	/* hold the prev error count captured on the last command execution */
	static long long prev_err_count = 0;
	int res = 0;
	if (cmd) {
		if ((server.stat_total_error_replies - prev_err_count) > 0) {
			if (flags & ERROR_COMMAND_REJECTED) {
				cmd->rejected_calls++;
				res = 1;
			}
			else if (flags & ERROR_COMMAND_FAILED) {
				cmd->failed_calls++;
				res = 1;
			}
		}
	}
	prev_err_count = server.stat_total_error_replies;
	return res;
}

/* Call() is the core of Redis execution of a command.
 *
 * The following flags can be passed:
 * CMD_CALL_NONE        No flags.
 * CMD_CALL_SLOWLOG     Check command speed and log in the slow log if needed.
 * CMD_CALL_STATS       Populate command stats.
 * CMD_CALL_PROPAGATE_AOF   Append command to AOF if it modified the dataset
 *                          or if the client flags are forcing propagation.
 * CMD_CALL_PROPAGATE_REPL  Send command to slaves if it modified the dataset
 *                          or if the client flags are forcing propagation.
 * CMD_CALL_PROPAGATE   Alias for PROPAGATE_AOF|PROPAGATE_REPL.
 * CMD_CALL_FULL        Alias for SLOWLOG|STATS|PROPAGATE.
 *
 * The exact propagation behavior depends on the client flags.
 * Specifically:
 *
 * 1. If the client flags CLIENT_FORCE_AOF or CLIENT_FORCE_REPL are set
 *    and assuming the corresponding CMD_CALL_PROPAGATE_AOF/REPL is set
 *    in the call flags, then the command is propagated even if the
 *    dataset was not affected by the command.
 * 2. If the client flags CLIENT_PREVENT_REPL_PROP or CLIENT_PREVENT_AOF_PROP
 *    are set, the propagation into AOF or to slaves is not performed even
 *    if the command modified the dataset.
 *
 * Note that regardless of the client flags, if CMD_CALL_PROPAGATE_AOF
 * or CMD_CALL_PROPAGATE_REPL are not set, then respectively AOF or
 * slaves propagation will never occur.
 *
 * Client flags are modified by the implementation of a given command
 * using the following API:
 *
 * forceCommandPropagation(client *c, int flags);
 * preventCommandPropagation(client *c);
 * preventCommandAOF(client *c);
 * preventCommandReplication(client *c);
 *
 */
void call(client *c, int flags) {
	long long dirty;
	uint64_t client_old_flags = c->flags;
	struct redisCommand *real_cmd = c->realcmd;

	/* Initialization: clear the flags that must be set by the command on
	 * demand, and initialize the array for additional commands propagation. */
	c->flags &= ~(CLIENT_FORCE_AOF | CLIENT_FORCE_REPL | CLIENT_PREVENT_PROP);

	/* Redis core is in charge of propagation when the first entry point
	 * of call() is processCommand().
	 * The only other option to get to call() without having processCommand
	 * as an entry point is if a module triggers RM_Call outside of call()
	 * context (for example, in a timer).
	 * In that case, the module is in charge of propagation.
	 *
	 * Because call() is re-entrant we have to cache and restore
	 * server.core_propagates. */
	int prev_core_propagates = server.core_propagates;
	if (!server.core_propagates && !(flags & CMD_CALL_FROM_MODULE))
		server.core_propagates = 1;

	/* Call the command. */
	incrCommandStatsOnError(NULL, 0);

	const long long call_timer = ustime();

	/* Update cache time, in case we have nested calls we want to
	 * update only on the first call*/
	if (server.fixed_time_expire++ == 0) {
		updateCachedTimeWithUs(0, call_timer);
	}

	monotime monotonic_start = 0;
	if (monotonicGetType() == MONOTONIC_CLOCK_HW)
		monotonic_start = getMonotonicUs();

	server.in_nested_call++;
	c->cmd->proc(c);
	server.in_nested_call--;

	/* In order to avoid performance implication due to querying the clock using a system call 3 times,
	 * we use a monotonic clock, when we are sure its cost is very low, and fall back to non-monotonic call otherwise. */
	ustime_t duration;
	if (monotonicGetType() == MONOTONIC_CLOCK_HW)
		duration = getMonotonicUs() - monotonic_start;
	else
		duration = ustime() - call_timer;

	c->duration = duration;
	/* Update failed command calls if required. */

	if (!incrCommandStatsOnError(real_cmd, ERROR_COMMAND_FAILED) && c->deferred_reply_errors) {
		/* When call is used from a module client, error stats, and total_error_replies
		 * isn't updated since these errors, if handled by the module, are internal,
		 * and not reflected to users. however, the commandstats does show these calls
		 * (made by RM_Call), so it should log if they failed or succeeded. */
		real_cmd->failed_calls++;
	}

	/* After executing command, we will close the client after writing entire
	 * reply if it is set 'CLIENT_CLOSE_AFTER_COMMAND' flag. */
	if (c->flags & CLIENT_CLOSE_AFTER_COMMAND) {
		c->flags &= ~CLIENT_CLOSE_AFTER_COMMAND;
		c->flags |= CLIENT_CLOSE_AFTER_REPLY;
	}

	/* When EVAL is called loading the AOF we don't want commands called
	 * from Lua to go into the slowlog or to populate statistics. */
	if (server.loading && c->flags & CLIENT_SCRIPT)
		flags &= ~(CMD_CALL_SLOWLOG | CMD_CALL_STATS);

	/* If the caller is Lua, we want to force the EVAL caller to propagate
	 * the script if the command flag or client flag are forcing the
	 * propagation. */
	if (c->flags & CLIENT_SCRIPT && server.script_caller) {
		if (c->flags & CLIENT_FORCE_REPL)
			server.script_caller->flags |= CLIENT_FORCE_REPL;
		if (c->flags & CLIENT_FORCE_AOF)
			server.script_caller->flags |= CLIENT_FORCE_AOF;
	}

	/* Note: the code below uses the real command that was executed
	 * c->cmd and c->lastcmd may be different, in case of MULTI-EXEC or
	 * re-written commands such as EXPIRE, GEOADD, etc. */

	 /* Record the latency this command induced on the main thread.
	  * unless instructed by the caller not to log. (happens when processing
	  * a MULTI-EXEC from inside an AOF). */
	if (flags & CMD_CALL_SLOWLOG) {
		char *latency_event = (real_cmd->flags & CMD_FAST) ?
			"fast-command" : "command";
		latencyAddSampleIfNeeded(latency_event, duration / 1000);
	}

	/* Log the command into the Slow log if needed.
	 * If the client is blocked we will handle slowlog when it is unblocked. */
	if ((flags & CMD_CALL_SLOWLOG) && !(c->flags & CLIENT_BLOCKED))
		slowlogPushCurrentCommand(c, real_cmd, duration);

	/* Clear the original argv.
	 * If the client is blocked we will handle slowlog when it is unblocked. */
	if (!(c->flags & CLIENT_BLOCKED))
		freeClientOriginalArgv(c);

	/* populate the per-command statistics that we show in INFO commandstats. */
	if (flags & CMD_CALL_STATS) {
		real_cmd->microseconds += duration;
		real_cmd->calls++;
	}

	/* Restore the old replication flags, since call() can be executed
	 * recursively. */
	c->flags &= ~(CLIENT_FORCE_AOF | CLIENT_FORCE_REPL | CLIENT_PREVENT_PROP);
	c->flags |= client_old_flags &
		(CLIENT_FORCE_AOF | CLIENT_FORCE_REPL | CLIENT_PREVENT_PROP);

	/* If the client has keys tracking enabled for client side caching,
	 * make sure to remember the keys it fetched via this command. Scripting
	 * works a bit differently, where if the scripts executes any read command, it
	 * remembers all of the declared keys from the script. */
	if ((c->cmd->flags & CMD_READONLY) && (c->cmd->proc != evalRoCommand)
		&& (c->cmd->proc != evalShaRoCommand) && (c->cmd->proc != fcallroCommand))
	{
		client *caller = (c->flags & CLIENT_SCRIPT && server.script_caller) ?
			server.script_caller : c;
		if (caller->flags & CLIENT_TRACKING &&
			!(caller->flags & CLIENT_TRACKING_BCAST))
		{
			trackingRememberKeys(caller);
		}
	}

	server.fixed_time_expire--;
	server.stat_numcommands++;

	/* Record peak memory after each command and before the eviction that runs
	 * before the next command. */
	size_t zmalloc_used = zmalloc_used_memory();
	if (zmalloc_used > server.stat_peak_memory)
		server.stat_peak_memory = zmalloc_used;

	/* Do some maintenance job and cleanup */
	afterCommand(c);

	/* Client pause takes effect after a transaction has finished. This needs
	 * to be located after everything is propagated. */
	if (!server.in_exec && server.client_pause_in_transaction) {
		server.client_pause_in_transaction = 0;
	}

	server.core_propagates = prev_core_propagates;
}

/* Used when a command that is ready for execution needs to be rejected, due to
 * various pre-execution checks. it returns the appropriate error to the client.
 * If there's a transaction is flags it as dirty, and if the command is EXEC,
 * it aborts the transaction.
 * Note: 'reply' is expected to end with \r\n */
void rejectCommand(client *c, robj *reply) {
	if (c->cmd) c->cmd->rejected_calls++;
	if (c->cmd && c->cmd->proc == execCommand) {
	}
	else {
		/* using addReplyError* rather than addReply so that the error can be logged. */
		addReplyErrorObject(c, reply);
	}
}

void rejectCommandSds(client *c, sds s) {
	if (c->cmd) c->cmd->rejected_calls++;
	if (c->cmd && c->cmd->proc == execCommand) {
		sdsfree(s);
	}
	else {
		/* The following frees 's'. */
		addReplyErrorSds(c, s);
	}
}

void rejectCommandFormat(client *c, const char *fmt, ...) {
	va_list ap;
	va_start(ap, fmt);
	sds s = sdscatvprintf(sdsempty(), fmt, ap);
	va_end(ap);
	/* Make sure there are no newlines in the string, otherwise invalid protocol
	 * is emitted (The args come from the user, they may contain any character). */
	sdsmapchars(s, "\r\n", "  ", 2);
	rejectCommandSds(c, s);
}

/* This is called after a command in call, we can do some maintenance job in it. */
void afterCommand(client *c) {
	UNUSED(c);
	if (!server.in_nested_call) {
		/* If we are at the top-most call() we can propagate what we accumulated.
		 * Should be done before trackingHandlePendingKeyInvalidations so that we
		 * reply to client before invalidating cache (makes more sense) */
		if (server.core_propagates)
			propagatePendingCommands();
		/* Flush pending invalidation messages only when we are not in nested call.
		 * So the messages are not interleaved with transaction response. */
		trackingHandlePendingKeyInvalidations();
	}
}

/* Check if c->cmd exists, fills `err` with details in case it doesn't.
 * Return 1 if exists. */
int commandCheckExistence(client *c, sds *err) {
	if (c->cmd)
		return 1;
	if (!err)
		return 0;
	if (isContainerCommandBySds(c->argv[0]->ptr)) {
		/* If we can't find the command but argv[0] by itself is a command
		 * it means we're dealing with an invalid subcommand. Print Help. */
		sds cmd = sdsnew((char *)c->argv[0]->ptr);
		sdstoupper(cmd);
		*err = sdsnew(NULL);
		*err = sdscatprintf(*err, "unknown subcommand '%.128s'. Try %s HELP.",
			(char *)c->argv[1]->ptr, cmd);
		sdsfree(cmd);
	}
	else {
		sds args = sdsempty();
		int i;
		for (i = 1; i < c->argc && sdslen(args) < 128; i++)
			args = sdscatprintf(args, "'%.*s' ", 128 - (int)sdslen(args), (char*)c->argv[i]->ptr);
		*err = sdsnew(NULL);
		*err = sdscatprintf(*err, "unknown command '%.128s', with args beginning with: %s",
			(char*)c->argv[0]->ptr, args);
		sdsfree(args);
	}
	/* Make sure there are no newlines in the string, otherwise invalid protocol
	 * is emitted (The args come from the user, they may contain any character). */
	sdsmapchars(*err, "\r\n", "  ", 2);
	return 0;
}

/* Check if c->argc is valid for c->cmd, fills `err` with details in case it isn't.
 * Return 1 if valid. */
int commandCheckArity(client *c, sds *err) {
	if ((c->cmd->arity > 0 && c->cmd->arity != c->argc) ||
		(c->argc < -c->cmd->arity))
	{
		if (err) {
			*err = sdsnew(NULL);
			*err = sdscatprintf(*err, "wrong number of arguments for '%s' command", c->cmd->fullname);
		}
		return 0;
	}

	return 1;
}

/* If we're executing a script, try to extract a set of command flags from
 * it, in case it declared them. Note this is just an attempt, we don't yet
 * know the script command is well formed.*/
uint64_t getCommandFlags(client *c) {
	uint64_t cmd_flags = c->cmd->flags;
	return cmd_flags;
}

/* If this function gets called we already read a whole
 * command, arguments are in the client argv/argc fields.
 * processCommand() execute the command or prepare the
 * server for a bulk read from the client.
 *
 * If C_OK is returned the client is still alive and valid and
 * other operations can be performed by the caller. Otherwise
 * if C_ERR is returned the client was destroyed (i.e. after QUIT). */
int processCommand(client *c) {
	/* Handle possible security attacks. */
	if (!strcasecmp(c->argv[0]->ptr, "host:") || !strcasecmp(c->argv[0]->ptr, "post")) {
		securityWarningCommand(c);
		return C_ERR;
	}

	/* If we're inside a module blocked context yielding that wants to avoid
	 * processing clients, postpone the command. */
	if (server.busy_module_yield_flags != BUSY_MODULE_YIELD_NONE &&
		!(server.busy_module_yield_flags & BUSY_MODULE_YIELD_CLIENTS))
	{
		c->bpop.timeout = 0;
		blockClient(c, BLOCKED_POSTPONE);
		return C_OK;
	}

	/* Now lookup the command and check ASAP about trivial error conditions
	 * such as wrong arity, bad command name and so forth. */
	c->cmd = c->lastcmd = c->realcmd = lookupCommand(c->argv, c->argc);
	sds err;
	if (!commandCheckExistence(c, &err)) {
		rejectCommandSds(c, err);
		return C_OK;
	}
	if (!commandCheckArity(c, &err)) {
		rejectCommandSds(c, err);
		return C_OK;
	}

	/* Check if the command is marked as protected and the relevant configuration allows it */
	if (c->cmd->flags & CMD_PROTECTED) {
		if ((c->cmd->proc == debugCommand && !allowProtectedAction(server.enable_debug_cmd, c)) ||
			(c->cmd->proc == moduleCommand && !allowProtectedAction(server.enable_module_cmd, c)))
		{
			rejectCommandFormat(c, "%s command not allowed. If the %s option is set to \"local\", "
				"you can run it from a local connection, otherwise you need to set this option "
				"in the configuration file, and then restart the server.",
				c->cmd->proc == debugCommand ? "DEBUG" : "MODULE",
				c->cmd->proc == debugCommand ? "enable-debug-command" : "enable-module-command");
			return C_OK;

		}
	}

	uint64_t cmd_flags = getCommandFlags(c);

	int is_read_command = (cmd_flags & CMD_READONLY) ||
		(c->cmd->proc == execCommand && (c->mstate.cmd_flags & CMD_READONLY));
	int is_write_command = (cmd_flags & CMD_WRITE) ||
		(c->cmd->proc == execCommand && (c->mstate.cmd_flags & CMD_WRITE));
	int is_denyoom_command = (cmd_flags & CMD_DENYOOM) ||
		(c->cmd->proc == execCommand && (c->mstate.cmd_flags & CMD_DENYOOM));
	int is_denystale_command = !(cmd_flags & CMD_STALE) ||
		(c->cmd->proc == execCommand && (c->mstate.cmd_inv_flags & CMD_STALE));
	int is_denyloading_command = !(cmd_flags & CMD_LOADING) ||
		(c->cmd->proc == execCommand && (c->mstate.cmd_inv_flags & CMD_LOADING));
	int is_may_replicate_command = (cmd_flags & (CMD_WRITE | CMD_MAY_REPLICATE)) ||
		(c->cmd->proc == execCommand && (c->mstate.cmd_flags & (CMD_WRITE | CMD_MAY_REPLICATE)));
	int is_deny_async_loading_command = (cmd_flags & CMD_NO_ASYNC_LOADING) ||
		(c->cmd->proc == execCommand && (c->mstate.cmd_flags & CMD_NO_ASYNC_LOADING));
	if (authRequired(c)) {
		/* AUTH and HELLO and no auth commands are valid even in
		 * non-authenticated state. */
		if (!(c->cmd->flags & CMD_NO_AUTH)) {
			rejectCommand(c, shared.noautherr);
			return C_OK;
		}
	}

	if (c->flags & CLIENT_MULTI && c->cmd->flags & CMD_NO_MULTI) {
		rejectCommandFormat(c, "Command not allowed inside a transaction");
		return C_OK;
	}

	/* Check if the user can run this command according to the current
	 * ACLs. */
	int acl_errpos;
	int acl_retval = ACLCheckAllPerm(c, &acl_errpos);
	if (acl_retval != ACL_OK) {
		addACLLogEntry(c, acl_retval, (c->flags & CLIENT_MULTI) ? ACL_LOG_CTX_MULTI : ACL_LOG_CTX_TOPLEVEL, acl_errpos, NULL, NULL);
		switch (acl_retval) {
		case ACL_DENIED_CMD:
		{
			rejectCommandFormat(c,
				"-NOPERM this user has no permissions to run "
				"the '%s' command", c->cmd->fullname);
			break;
		}
		case ACL_DENIED_KEY:
			rejectCommandFormat(c,
				"-NOPERM this user has no permissions to access "
				"one of the keys used as arguments");
			break;
		case ACL_DENIED_CHANNEL:
			rejectCommandFormat(c,
				"-NOPERM this user has no permissions to access "
				"one of the channels used as arguments");
			break;
		default:
			rejectCommandFormat(c, "no permission");
			break;
		}
		return C_OK;
	}



	/* Disconnect some clients if total clients memory is too high. We do this
	 * before key eviction, after the last command was executed and consumed
	 * some client output buffer memory. */
	evictClients();
	if (server.current_client == NULL) {
		/* If we evicted ourself then abort processing the command */
		return C_ERR;
	}

	/* Handle the maxmemory directive.
	 *
	 * Note that we do not want to reclaim memory if we are here re-entering
	 * the event loop since there is a busy Lua script running in timeout
	 * condition, to avoid mixing the propagation of scripts with the
	 * propagation of DELs due to eviction. */
	if (server.maxmemory) {
		int out_of_memory = 0;

		/* performEvictions may evict keys, so we need flush pending tracking
		 * invalidation keys. If we don't do this, we may get an invalidation
		 * message after we perform operation on the key, where in fact this
		 * message belongs to the old value of the key before it gets evicted.*/
		trackingHandlePendingKeyInvalidations();

		/* performEvictions may flush slave output buffers. This may result
		 * in a slave, that may be the active client, to be freed. */
		if (server.current_client == NULL) return C_ERR;

		int reject_cmd_on_oom = is_denyoom_command;
		/* If client is in MULTI/EXEC context, queuing may consume an unlimited
		 * amount of memory, so we want to stop that.
		 * However, we never want to reject DISCARD, or even EXEC (unless it
		 * contains denied commands, in which case is_denyoom_command is already
		 * set. */
		if (c->flags & CLIENT_MULTI &&
			c->cmd->proc != execCommand &&
			c->cmd->proc != discardCommand &&
			c->cmd->proc != quitCommand &&
			c->cmd->proc != resetCommand) {
			reject_cmd_on_oom = 1;
		}

		if (out_of_memory && reject_cmd_on_oom) {
			rejectCommand(c, shared.oomerr);
			return C_OK;
		}

		/* Save out_of_memory result at command start, otherwise if we check OOM
		 * in the first write within script, memory used by lua stack and
		 * arguments might interfere. We need to save it for EXEC and module
		 * calls too, since these can call EVAL, but avoid saving it during an
		 * interrupted / yielding busy script / module. */
		server.pre_command_oom_state = out_of_memory;
	}

	/* Make sure to use a reasonable amount of memory for client side
	 * caching metadata. */
	if (server.tracking_clients) trackingLimitUsedSlots();

	/* Only allow a subset of commands in the context of Pub/Sub if the
	 * connection is in RESP2 mode. With RESP3 there are no limits. */
	if ((c->flags & CLIENT_PUBSUB && c->resp == 2) &&
		c->cmd->proc != pingCommand &&
		c->cmd->proc != subscribeCommand &&
		c->cmd->proc != ssubscribeCommand &&
		c->cmd->proc != unsubscribeCommand &&
		c->cmd->proc != sunsubscribeCommand &&
		c->cmd->proc != psubscribeCommand &&
		c->cmd->proc != punsubscribeCommand &&
		c->cmd->proc != quitCommand &&
		c->cmd->proc != resetCommand) {
		rejectCommandFormat(c,
			"Can't execute '%s': only (P|S)SUBSCRIBE / "
			"(P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
			c->cmd->fullname);
		return C_OK;
	}

	call(c, CMD_CALL_FULL);
	if (listLength(server.ready_keys))
		handleClientsBlockedOnKeys();

	return C_OK;
}

/* ====================== Error lookup and execution ===================== */

void incrementErrorCount(const char *fullerr, size_t namelen) {
	struct redisError *error = raxFind(server.errors, (unsigned char*)fullerr, namelen);
	if (error == raxNotFound) {
		error = zmalloc(sizeof(*error));
		error->count = 0;
		raxInsert(server.errors, (unsigned char*)fullerr, namelen, error, NULL);
	}
	error->count++;
}

/*================================== Commands =============================== */

/* Sometimes Redis cannot accept write commands because there is a persistence
 * error with the RDB or AOF file, and Redis is configured in order to stop
 * accepting writes in such situation. This function returns if such a
 * condition is active, and the type of the condition.
 *
 * Function return values:
 *
 * DISK_ERROR_TYPE_NONE:    No problems, we can accept writes.
 * DISK_ERROR_TYPE_AOF:     Don't accept writes: AOF errors.
 * DISK_ERROR_TYPE_RDB:     Don't accept writes: RDB errors.
 */

/* The PING command. It works in a different way if the client is in
 * in Pub/Sub mode. */
void pingCommand(client *c) {
	/* The command takes zero or one arguments. */
	if (c->argc > 2) {
		addReplyErrorArity(c);
		return;
	}

	if (c->flags & CLIENT_PUBSUB && c->resp == 2) {
		addReply(c, shared.mbulkhdr[2]);
		addReplyBulkCBuffer(c, "pong", 4);
		if (c->argc == 1)
			addReplyBulkCBuffer(c, "", 0);
		else
			addReplyBulk(c, c->argv[1]);
	}
	else {
		if (c->argc == 1)
			addReply(c, shared.pong);
		else
			addReplyBulk(c, c->argv[1]);
	}
}

void echoCommand(client *c) {
	addReplyBulk(c, c->argv[1]);
}

void timeCommand(client *c) {
	struct timeval tv;

	/* gettimeofday() can only fail if &tv is a bad address so we
	 * don't check for errors. */
	gettimeofday(&tv, NULL);
	addReplyArrayLen(c, 2);
	addReplyBulkLongLong(c, tv.tv_sec);
	addReplyBulkLongLong(c, tv.tv_usec);
}

typedef struct replyFlagNames {
	uint64_t flag;
	const char *name;
} replyFlagNames;

/* Helper function to output flags. */
void addReplyCommandFlags(client *c, uint64_t flags, replyFlagNames *replyFlags) {
	int count = 0, j = 0;
	/* Count them so we don't have to use deferred reply. */
	while (replyFlags[j].name) {
		if (flags & replyFlags[j].flag)
			count++;
		j++;
	}

	addReplySetLen(c, count);
	j = 0;
	while (replyFlags[j].name) {
		if (flags & replyFlags[j].flag)
			addReplyStatus(c, replyFlags[j].name);
		j++;
	}
}

void addReplyFlagsForCommand(client *c, struct redisCommand *cmd) {
	replyFlagNames flagNames[] = {
	    {CMD_WRITE,             "write"},
	    {CMD_READONLY,          "readonly"},
	    {CMD_DENYOOM,           "denyoom"},
	    {CMD_MODULE,            "module"},
	    {CMD_ADMIN,             "admin"},
	    {CMD_PUBSUB,            "pubsub"},
	    {CMD_NOSCRIPT,          "noscript"},
	    {CMD_BLOCKING,          "blocking"},
	    {CMD_LOADING,           "loading"},
	    {CMD_STALE,             "stale"},
	    {CMD_SKIP_MONITOR,      "skip_monitor"},
	    {CMD_SKIP_SLOWLOG,      "skip_slowlog"},
	    {CMD_ASKING,            "asking"},
	    {CMD_FAST,              "fast"},
	    {CMD_NO_AUTH,           "no_auth"},
	    /* {CMD_MAY_REPLICATE,     "may_replicate"},, Hidden on purpose */
	    /* {CMD_SENTINEL,          "sentinel"}, Hidden on purpose */
	    /* {CMD_ONLY_SENTINEL,     "only_sentinel"}, Hidden on purpose */
	    {CMD_NO_MANDATORY_KEYS, "no_mandatory_keys"},
	    /* {CMD_PROTECTED,         "protected"}, Hidden on purpose */
	    {CMD_NO_ASYNC_LOADING,  "no_async_loading"},
	    {CMD_NO_MULTI,          "no_multi"},
	    {CMD_MOVABLE_KEYS,      "movablekeys"},
	    {CMD_ALLOW_BUSY,        "allow_busy"},
	    /* {CMD_TOUCHES_ARBITRARY_KEYS,  "TOUCHES_ARBITRARY_KEYS"}, Hidden on purpose */
	    {0,NULL}
	};
	addReplyCommandFlags(c, cmd->flags, flagNames);
}

void addReplyDocFlagsForCommand(client *c, struct redisCommand *cmd) {
	replyFlagNames docFlagNames[] = {
	    {CMD_DOC_DEPRECATED,         "deprecated"},
	    {CMD_DOC_SYSCMD,             "syscmd"},
	    {0,NULL}
	};
	addReplyCommandFlags(c, cmd->doc_flags, docFlagNames);
}

void addReplyFlagsForKeyArgs(client *c, uint64_t flags) {
	replyFlagNames docFlagNames[] = {
	    {CMD_KEY_RO,              "RO"},
	    {CMD_KEY_RW,              "RW"},
	    {CMD_KEY_OW,              "OW"},
	    {CMD_KEY_RM,              "RM"},
	    {CMD_KEY_ACCESS,          "access"},
	    {CMD_KEY_UPDATE,          "update"},
	    {CMD_KEY_INSERT,          "insert"},
	    {CMD_KEY_DELETE,          "delete"},
	    {CMD_KEY_NOT_KEY,         "not_key"},
	    {CMD_KEY_INCOMPLETE,      "incomplete"},
	    {CMD_KEY_VARIABLE_FLAGS,  "variable_flags"},
	    {0,NULL}
	};
	addReplyCommandFlags(c, flags, docFlagNames);
}

/* Must match redisCommandArgType */
const char *ARG_TYPE_STR[] = {
    "string",
    "integer",
    "double",
    "key",
    "pattern",
    "unix-time",
    "pure-token",
    "oneof",
    "block",
};

void addReplyFlagsForArg(client *c, uint64_t flags) {
	replyFlagNames argFlagNames[] = {
	    {CMD_ARG_OPTIONAL,          "optional"},
	    {CMD_ARG_MULTIPLE,          "multiple"},
	    {CMD_ARG_MULTIPLE_TOKEN,    "multiple_token"},
	    {0,NULL}
	};
	addReplyCommandFlags(c, flags, argFlagNames);
}

void addReplyCommandArgList(client *c, struct redisCommandArg *args, int num_args) {
	addReplyArrayLen(c, num_args);
	for (int j = 0; j < num_args; j++) {
		/* Count our reply len so we don't have to use deferred reply. */
		long maplen = 2;
		if (args[j].key_spec_index != -1) maplen++;
		if (args[j].token) maplen++;
		if (args[j].summary) maplen++;
		if (args[j].since) maplen++;
		if (args[j].deprecated_since) maplen++;
		if (args[j].flags) maplen++;
		if (args[j].type == ARG_TYPE_ONEOF || args[j].type == ARG_TYPE_BLOCK)
			maplen++;
		addReplyMapLen(c, maplen);

		addReplyBulkCString(c, "name");
		addReplyBulkCString(c, args[j].name);

		addReplyBulkCString(c, "type");
		addReplyBulkCString(c, ARG_TYPE_STR[args[j].type]);

		if (args[j].key_spec_index != -1) {
			addReplyBulkCString(c, "key_spec_index");
			addReplyLongLong(c, args[j].key_spec_index);
		}
		if (args[j].token) {
			addReplyBulkCString(c, "token");
			addReplyBulkCString(c, args[j].token);
		}
		if (args[j].summary) {
			addReplyBulkCString(c, "summary");
			addReplyBulkCString(c, args[j].summary);
		}
		if (args[j].since) {
			addReplyBulkCString(c, "since");
			addReplyBulkCString(c, args[j].since);
		}
		if (args[j].deprecated_since) {
			addReplyBulkCString(c, "deprecated_since");
			addReplyBulkCString(c, args[j].deprecated_since);
		}
		if (args[j].flags) {
			addReplyBulkCString(c, "flags");
			addReplyFlagsForArg(c, args[j].flags);
		}
		if (args[j].type == ARG_TYPE_ONEOF || args[j].type == ARG_TYPE_BLOCK) {
			addReplyBulkCString(c, "arguments");
			addReplyCommandArgList(c, args[j].subargs, args[j].num_args);
		}
	}
}

/* Must match redisCommandRESP2Type */
const char *RESP2_TYPE_STR[] = {
    "simple-string",
    "error",
    "integer",
    "bulk-string",
    "null-bulk-string",
    "array",
    "null-array",
};

/* Must match redisCommandRESP3Type */
const char *RESP3_TYPE_STR[] = {
    "simple-string",
    "error",
    "integer",
    "double",
    "bulk-string",
    "array",
    "map",
    "set",
    "bool",
    "null",
};

void addReplyCommandHistory(client *c, struct redisCommand *cmd) {
	addReplySetLen(c, cmd->num_history);
	for (int j = 0; j < cmd->num_history; j++) {
		addReplyArrayLen(c, 2);
		addReplyBulkCString(c, cmd->history[j].since);
		addReplyBulkCString(c, cmd->history[j].changes);
	}
}

void addReplyCommandTips(client *c, struct redisCommand *cmd) {
	addReplySetLen(c, cmd->num_tips);
	for (int j = 0; j < cmd->num_tips; j++) {
		addReplyBulkCString(c, cmd->tips[j]);
	}
}

void addReplyCommandKeySpecs(client *c, struct redisCommand *cmd) {
	addReplySetLen(c, cmd->key_specs_num);
	for (int i = 0; i < cmd->key_specs_num; i++) {
		int maplen = 3;
		if (cmd->key_specs[i].notes) maplen++;

		addReplyMapLen(c, maplen);

		if (cmd->key_specs[i].notes) {
			addReplyBulkCString(c, "notes");
			addReplyBulkCString(c, cmd->key_specs[i].notes);
		}

		addReplyBulkCString(c, "flags");
		addReplyFlagsForKeyArgs(c, cmd->key_specs[i].flags);

		addReplyBulkCString(c, "begin_search");
		switch (cmd->key_specs[i].begin_search_type) {
		case KSPEC_BS_UNKNOWN:
			addReplyMapLen(c, 2);
			addReplyBulkCString(c, "type");
			addReplyBulkCString(c, "unknown");

			addReplyBulkCString(c, "spec");
			addReplyMapLen(c, 0);
			break;
		case KSPEC_BS_INDEX:
			addReplyMapLen(c, 2);
			addReplyBulkCString(c, "type");
			addReplyBulkCString(c, "index");

			addReplyBulkCString(c, "spec");
			addReplyMapLen(c, 1);
			addReplyBulkCString(c, "index");
			addReplyLongLong(c, cmd->key_specs[i].bs.index.pos);
			break;
		case KSPEC_BS_KEYWORD:
			addReplyMapLen(c, 2);
			addReplyBulkCString(c, "type");
			addReplyBulkCString(c, "keyword");

			addReplyBulkCString(c, "spec");
			addReplyMapLen(c, 2);
			addReplyBulkCString(c, "keyword");
			addReplyBulkCString(c, cmd->key_specs[i].bs.keyword.keyword);
			addReplyBulkCString(c, "startfrom");
			addReplyLongLong(c, cmd->key_specs[i].bs.keyword.startfrom);
			break;
		default:
			serverPanic("Invalid begin_search key spec type %d", cmd->key_specs[i].begin_search_type);
		}

		addReplyBulkCString(c, "find_keys");
		switch (cmd->key_specs[i].find_keys_type) {
		case KSPEC_FK_UNKNOWN:
			addReplyMapLen(c, 2);
			addReplyBulkCString(c, "type");
			addReplyBulkCString(c, "unknown");

			addReplyBulkCString(c, "spec");
			addReplyMapLen(c, 0);
			break;
		case KSPEC_FK_RANGE:
			addReplyMapLen(c, 2);
			addReplyBulkCString(c, "type");
			addReplyBulkCString(c, "range");

			addReplyBulkCString(c, "spec");
			addReplyMapLen(c, 3);
			addReplyBulkCString(c, "lastkey");
			addReplyLongLong(c, cmd->key_specs[i].fk.range.lastkey);
			addReplyBulkCString(c, "keystep");
			addReplyLongLong(c, cmd->key_specs[i].fk.range.keystep);
			addReplyBulkCString(c, "limit");
			addReplyLongLong(c, cmd->key_specs[i].fk.range.limit);
			break;
		case KSPEC_FK_KEYNUM:
			addReplyMapLen(c, 2);
			addReplyBulkCString(c, "type");
			addReplyBulkCString(c, "keynum");

			addReplyBulkCString(c, "spec");
			addReplyMapLen(c, 3);
			addReplyBulkCString(c, "keynumidx");
			addReplyLongLong(c, cmd->key_specs[i].fk.keynum.keynumidx);
			addReplyBulkCString(c, "firstkey");
			addReplyLongLong(c, cmd->key_specs[i].fk.keynum.firstkey);
			addReplyBulkCString(c, "keystep");
			addReplyLongLong(c, cmd->key_specs[i].fk.keynum.keystep);
			break;
		default:
			serverPanic("Invalid find_keys key spec type %d", cmd->key_specs[i].begin_search_type);
		}
	}
}

/* Reply with an array of sub-command using the provided reply callback. */
void addReplyCommandSubCommands(client *c, struct redisCommand *cmd, void(*reply_function)(client*, struct redisCommand*), int use_map) {
	if (!cmd->subcommands_dict) {
		addReplySetLen(c, 0);
		return;
	}

	if (use_map)
		addReplyMapLen(c, dictSize(cmd->subcommands_dict));
	else
		addReplyArrayLen(c, dictSize(cmd->subcommands_dict));
	dictEntry *de;
	dictIterator *di = dictGetSafeIterator(cmd->subcommands_dict);
	while ((de = dictNext(di)) != NULL) {
		struct redisCommand *sub = (struct redisCommand *)dictGetVal(de);
		if (use_map)
			addReplyBulkCBuffer(c, sub->fullname, sdslen(sub->fullname));
		reply_function(c, sub);
	}
	dictReleaseIterator(di);
}

/* Must match redisCommandGroup */
const char *COMMAND_GROUP_STR[] = {
    "generic",
    "string",
    "list",
    "set",
    "sorted-set",
    "hash",
    "pubsub",
    "transactions",
    "connection",
    "server",
    "scripting",
    "hyperloglog",
    "cluster",
    "sentinel",
    "geo",
    "stream",
    "bitmap",
    "module"
};

/* Output the representation of a Redis command. Used by the COMMAND command and COMMAND INFO. */
void addReplyCommandInfo(client *c, struct redisCommand *cmd) {
	if (!cmd) {
		addReplyNull(c);
	}
	else {
		int firstkey = 0, lastkey = 0, keystep = 0;
		if (cmd->legacy_range_key_spec.begin_search_type != KSPEC_BS_INVALID) {
			firstkey = cmd->legacy_range_key_spec.bs.index.pos;
			lastkey = cmd->legacy_range_key_spec.fk.range.lastkey;
			if (lastkey >= 0)
				lastkey += firstkey;
			keystep = cmd->legacy_range_key_spec.fk.range.keystep;
		}

		addReplyArrayLen(c, 10);
		addReplyBulkCBuffer(c, cmd->fullname, sdslen(cmd->fullname));
		addReplyLongLong(c, cmd->arity);
		addReplyFlagsForCommand(c, cmd);
		addReplyLongLong(c, firstkey);
		addReplyLongLong(c, lastkey);
		addReplyLongLong(c, keystep);
		addReplyCommandCategories(c, cmd);
		addReplyCommandTips(c, cmd);
		addReplyCommandKeySpecs(c, cmd);
		addReplyCommandSubCommands(c, cmd, addReplyCommandInfo, 0);
	}
}

/* Output the representation of a Redis command. Used by the COMMAND DOCS. */
void addReplyCommandDocs(client *c, struct redisCommand *cmd) {
	/* Count our reply len so we don't have to use deferred reply. */
	long maplen = 1;
	if (cmd->summary) maplen++;
	if (cmd->since) maplen++;
	if (cmd->flags & CMD_MODULE) maplen++;
	if (cmd->complexity) maplen++;
	if (cmd->doc_flags) maplen++;
	if (cmd->deprecated_since) maplen++;
	if (cmd->replaced_by) maplen++;
	if (cmd->history) maplen++;
	if (cmd->args) maplen++;
	if (cmd->subcommands_dict) maplen++;
	addReplyMapLen(c, maplen);

	if (cmd->summary) {
		addReplyBulkCString(c, "summary");
		addReplyBulkCString(c, cmd->summary);
	}
	if (cmd->since) {
		addReplyBulkCString(c, "since");
		addReplyBulkCString(c, cmd->since);
	}

	/* Always have the group, for module commands the group is always "module". */
	addReplyBulkCString(c, "group");
	addReplyBulkCString(c, COMMAND_GROUP_STR[cmd->group]);

	if (cmd->complexity) {
		addReplyBulkCString(c, "complexity");
		addReplyBulkCString(c, cmd->complexity);
	}
	if (cmd->doc_flags) {
		addReplyBulkCString(c, "doc_flags");
		addReplyDocFlagsForCommand(c, cmd);
	}
	if (cmd->deprecated_since) {
		addReplyBulkCString(c, "deprecated_since");
		addReplyBulkCString(c, cmd->deprecated_since);
	}
	if (cmd->replaced_by) {
		addReplyBulkCString(c, "replaced_by");
		addReplyBulkCString(c, cmd->replaced_by);
	}
	if (cmd->history) {
		addReplyBulkCString(c, "history");
		addReplyCommandHistory(c, cmd);
	}
	if (cmd->args) {
		addReplyBulkCString(c, "arguments");
		addReplyCommandArgList(c, cmd->args, cmd->num_args);
	}
	if (cmd->subcommands_dict) {
		addReplyBulkCString(c, "subcommands");
		addReplyCommandSubCommands(c, cmd, addReplyCommandDocs, 1);
	}
}

/* Helper for COMMAND GETKEYS and GETKEYSANDFLAGS */
void getKeysSubcommandImpl(client *c, int with_flags) {
	struct redisCommand *cmd = lookupCommand(c->argv + 2, c->argc - 2);
	getKeysResult result = GETKEYS_RESULT_INIT;
	int j;

	if (!cmd) {
		addReplyError(c, "Invalid command specified");
		return;
	}
	else if ((cmd->arity > 0 && cmd->arity != c->argc - 2) ||
		((c->argc - 2) < -cmd->arity))
	{
		addReplyError(c, "Invalid number of arguments specified for command");
		return;
	}
	else {
		addReplyArrayLen(c, result.numkeys);
		for (j = 0; j < result.numkeys; j++) {
			if (!with_flags) {
				addReplyBulk(c, c->argv[result.keys[j].pos + 2]);
			}
			else {
				addReplyArrayLen(c, 2);
				addReplyBulk(c, c->argv[result.keys[j].pos + 2]);
				addReplyFlagsForKeyArgs(c, result.keys[j].flags);
			}
		}
	}
}

/* COMMAND GETKEYSANDFLAGS cmd arg1 arg2 ... */
void commandGetKeysAndFlagsCommand(client *c) {
	getKeysSubcommandImpl(c, 1);
}

/* COMMAND GETKEYS cmd arg1 arg2 ... */
void getKeysSubcommand(client *c) {
	getKeysSubcommandImpl(c, 0);
}

/* COMMAND (no args) */
void commandCommand(client *c) {
	dictIterator *di;
	dictEntry *de;

	addReplyArrayLen(c, dictSize(server.commands));
	di = dictGetIterator(server.commands);
	while ((de = dictNext(di)) != NULL) {
		addReplyCommandInfo(c, dictGetVal(de));
	}
	dictReleaseIterator(di);
}

/* COMMAND COUNT */
void commandCountCommand(client *c) {
	addReplyLongLong(c, dictSize(server.commands));
}

typedef enum {
	COMMAND_LIST_FILTER_MODULE,
	COMMAND_LIST_FILTER_ACLCAT,
	COMMAND_LIST_FILTER_PATTERN,
} commandListFilterType;

typedef struct {
	commandListFilterType type;
	sds arg;
	struct {
		int valid;
		union {
			uint64_t aclcat;
			void *module_handle;
		} u;
	} cache;
} commandListFilter;

int shouldFilterFromCommandList(struct redisCommand *cmd, commandListFilter *filter) {
	switch (filter->type) {
	case (COMMAND_LIST_FILTER_ACLCAT): {
		if (!filter->cache.valid) {
			filter->cache.u.aclcat = ACLGetCommandCategoryFlagByName(filter->arg);
			filter->cache.valid = 1;
		}
		uint64_t cat = filter->cache.u.aclcat;
		if (cat == 0)
			return 1; /* Invalid ACL category */
		return (!(cmd->acl_categories & cat));
		break;
	}
	case (COMMAND_LIST_FILTER_PATTERN):
		return !stringmatchlen(filter->arg, sdslen(filter->arg), cmd->fullname, sdslen(cmd->fullname), 1);
	default:
		serverPanic("Invalid filter type %d", filter->type);
	}
}

/* COMMAND LIST FILTERBY (MODULE <module-name>|ACLCAT <cat>|PATTERN <pattern>) */
void commandListWithFilter(client *c, dict *commands, commandListFilter filter, int *numcmds) {
	dictEntry *de;
	dictIterator *di = dictGetIterator(commands);

	while ((de = dictNext(di)) != NULL) {
		struct redisCommand *cmd = dictGetVal(de);
		if (!shouldFilterFromCommandList(cmd, &filter)) {
			addReplyBulkCBuffer(c, cmd->fullname, sdslen(cmd->fullname));
			(*numcmds)++;
		}

		if (cmd->subcommands_dict) {
			commandListWithFilter(c, cmd->subcommands_dict, filter, numcmds);
		}
	}
	dictReleaseIterator(di);
}

/* COMMAND LIST */
void commandListWithoutFilter(client *c, dict *commands, int *numcmds) {
	dictEntry *de;
	dictIterator *di = dictGetIterator(commands);

	while ((de = dictNext(di)) != NULL) {
		struct redisCommand *cmd = dictGetVal(de);
		addReplyBulkCBuffer(c, cmd->fullname, sdslen(cmd->fullname));
		(*numcmds)++;

		if (cmd->subcommands_dict) {
			commandListWithoutFilter(c, cmd->subcommands_dict, numcmds);
		}
	}
	dictReleaseIterator(di);
}

/* COMMAND LIST [FILTERBY (MODULE <module-name>|ACLCAT <cat>|PATTERN <pattern>)] */
void commandListCommand(client *c) {

	/* Parse options. */
	int i = 2, got_filter = 0;
	commandListFilter filter = { 0 };
	for (; i < c->argc; i++) {
		int moreargs = (c->argc - 1) - i; /* Number of additional arguments. */
		char *opt = c->argv[i]->ptr;
		if (!strcasecmp(opt, "filterby") && moreargs == 2) {
			char *filtertype = c->argv[i + 1]->ptr;
			if (!strcasecmp(filtertype, "module")) {
				filter.type = COMMAND_LIST_FILTER_MODULE;
			}
			else if (!strcasecmp(filtertype, "aclcat")) {
				filter.type = COMMAND_LIST_FILTER_ACLCAT;
			}
			else if (!strcasecmp(filtertype, "pattern")) {
				filter.type = COMMAND_LIST_FILTER_PATTERN;
			}
			else {
				addReplyErrorObject(c, shared.syntaxerr);
				return;
			}
			got_filter = 1;
			filter.arg = c->argv[i + 2]->ptr;
			i += 2;
		}
		else {
			addReplyErrorObject(c, shared.syntaxerr);
			return;
		}
	}

	int numcmds = 0;
	void *replylen = addReplyDeferredLen(c);

	if (got_filter) {
		commandListWithFilter(c, server.commands, filter, &numcmds);
	}
	else {
		commandListWithoutFilter(c, server.commands, &numcmds);
	}

	setDeferredArrayLen(c, replylen, numcmds);
}

/* COMMAND INFO [<command-name> ...] */
void commandInfoCommand(client *c) {
	int i;

	if (c->argc == 2) {
		dictIterator *di;
		dictEntry *de;
		addReplyArrayLen(c, dictSize(server.commands));
		di = dictGetIterator(server.commands);
		while ((de = dictNext(di)) != NULL) {
			addReplyCommandInfo(c, dictGetVal(de));
		}
		dictReleaseIterator(di);
	}
	else {
		addReplyArrayLen(c, c->argc - 2);
		for (i = 2; i < c->argc; i++) {
			addReplyCommandInfo(c, lookupCommandBySds(c->argv[i]->ptr));
		}
	}
}

/* COMMAND DOCS [command-name [command-name ...]] */
void commandDocsCommand(client *c) {
	int i;
	if (c->argc == 2) {
		/* Reply with an array of all commands */
		dictIterator *di;
		dictEntry *de;
		addReplyMapLen(c, dictSize(server.commands));
		di = dictGetIterator(server.commands);
		while ((de = dictNext(di)) != NULL) {
			struct redisCommand *cmd = dictGetVal(de);
			addReplyBulkCBuffer(c, cmd->fullname, sdslen(cmd->fullname));
			addReplyCommandDocs(c, cmd);
		}
		dictReleaseIterator(di);
	}
	else {
		/* Reply with an array of the requested commands (if we find them) */
		int numcmds = 0;
		void *replylen = addReplyDeferredLen(c);
		for (i = 2; i < c->argc; i++) {
			struct redisCommand *cmd = lookupCommandBySds(c->argv[i]->ptr);
			if (!cmd)
				continue;
			addReplyBulkCBuffer(c, cmd->fullname, sdslen(cmd->fullname));
			addReplyCommandDocs(c, cmd);
			numcmds++;
		}
		setDeferredMapLen(c, replylen, numcmds);
	}
}

/* COMMAND GETKEYS arg0 arg1 arg2 ... */
void commandGetKeysCommand(client *c) {
	getKeysSubcommand(c);
}

/* COMMAND HELP */
void commandHelpCommand(client *c) {
	const char *help[] = {
    "(no subcommand)",
    "    Return details about all Redis commands.",
    "COUNT",
    "    Return the total number of commands in this Redis server.",
    "LIST",
    "    Return a list of all commands in this Redis server.",
    "INFO [<command-name> ...]",
    "    Return details about multiple Redis commands.",
    "    If no command names are given, documentation details for all",
    "    commands are returned.",
    "DOCS [<command-name> ...]",
    "    Return documentation details about multiple Redis commands.",
    "    If no command names are given, documentation details for all",
    "    commands are returned.",
    "GETKEYS <full-command>",
    "    Return the keys from a full Redis command.",
    "GETKEYSANDFLAGS <full-command>",
    "    Return the keys and the access flags from a full Redis command.",
    NULL
	};

	addReplyHelp(c, help);
}

/* Convert an amount of bytes into a human readable string in the form
 * of 100B, 2G, 100M, 4K, and so forth. */
void bytesToHuman(char *s, unsigned long long n) {
	double d;

	if (n < 1024) {
		/* Bytes */
		sprintf(s, "%lluB", n);
	}
	else if (n < (1024 * 1024)) {
		d = (double)n / (1024);
		sprintf(s, "%.2fK", d);
	}
	else if (n < (1024LL * 1024 * 1024)) {
		d = (double)n / (1024 * 1024);
		sprintf(s, "%.2fM", d);
	}
	else if (n < (1024LL * 1024 * 1024 * 1024)) {
		d = (double)n / (1024LL * 1024 * 1024);
		sprintf(s, "%.2fG", d);
	}
	else if (n < (1024LL * 1024 * 1024 * 1024 * 1024)) {
		d = (double)n / (1024LL * 1024 * 1024 * 1024);
		sprintf(s, "%.2fT", d);
	}
	else if (n < (1024LL * 1024 * 1024 * 1024 * 1024 * 1024)) {
		d = (double)n / (1024LL * 1024 * 1024 * 1024 * 1024);
		sprintf(s, "%.2fP", d);
	}
	else {
		/* Let's hope we never need this */
		sprintf(s, "%lluB", n);
	}
}

/* Characters we sanitize on INFO output to maintain expected format. */
static char unsafe_info_chars[] = "#:\n\r";
static char unsafe_info_chars_substs[] = "____";   /* Must be same length as above */

/* Returns a sanitized version of s that contains no unsafe info string chars.
 * If no unsafe characters are found, simply returns s. Caller needs to
 * free tmp if it is non-null on return.
 */
const char *getSafeInfoString(const char *s, size_t len, char **tmp) {
	*tmp = NULL;
	if (mempbrk(s, len, unsafe_info_chars, sizeof(unsafe_info_chars) - 1)
		== NULL) return s;
	char *new = *tmp = zmalloc(len + 1);
	memcpy(new, s, len);
	new[len] = '\0';
	return memmapchars(new, len, unsafe_info_chars, unsafe_info_chars_substs,
		sizeof(unsafe_info_chars) - 1);
}

sds genRedisInfoStringCommandStats(sds info, dict *commands) {
	struct redisCommand *c;
	dictEntry *de;
	dictIterator *di;
	di = dictGetSafeIterator(commands);
	while ((de = dictNext(di)) != NULL) {
		char *tmpsafe;
		c = (struct redisCommand *) dictGetVal(de);
		if (c->calls || c->failed_calls || c->rejected_calls) {
			info = sdscatprintf(info,
				"cmdstat_%s:calls=%lld,usec=%lld,usec_per_call=%.2f"
				",rejected_calls=%lld,failed_calls=%lld\r\n",
				getSafeInfoString(c->fullname, sdslen(c->fullname), &tmpsafe), c->calls, c->microseconds,
				(c->calls == 0) ? 0 : ((float)c->microseconds / c->calls),
				c->rejected_calls, c->failed_calls);
			if (tmpsafe != NULL) zfree(tmpsafe);
		}
		if (c->subcommands_dict) {
			info = genRedisInfoStringCommandStats(info, c->subcommands_dict);
		}
	}
	dictReleaseIterator(di);

	return info;
}

sds genRedisInfoStringLatencyStats(sds info, dict *commands) {
	struct redisCommand *c;
	dictEntry *de;
	dictIterator *di;
	di = dictGetSafeIterator(commands);
	while ((de = dictNext(di)) != NULL) {
		char *tmpsafe;
		c = (struct redisCommand *) dictGetVal(de);
		if (c->subcommands_dict) {
			info = genRedisInfoStringLatencyStats(info, c->subcommands_dict);
		}
	}
	dictReleaseIterator(di);

	return info;
}

/* Takes a null terminated sections list, and adds them to the dict. */
void addInfoSectionsToDict(dict *section_dict, char **sections) {
	while (*sections) {
		sds section = sdsnew(*sections);
		if (dictAdd(section_dict, section, NULL) == DICT_ERR)
			sdsfree(section);
		sections++;
	}
}

/* Cached copy of the default sections, as an optimization. */
static dict *cached_default_info_sections = NULL;

void releaseInfoSectionDict(dict *sec) {
	if (sec != cached_default_info_sections)
		dictRelease(sec);
}

/* Create a dictionary with unique section names to be used by genRedisInfoString.
 * 'argv' and 'argc' are list of arguments for INFO.
 * 'defaults' is an optional null terminated list of default sections.
 * 'out_all' and 'out_everything' are optional.
 * The resulting dictionary should be released with releaseInfoSectionDict. */
dict *genInfoSectionDict(robj **argv, int argc, char **defaults, int *out_all, int *out_everything) {
	char *default_sections[] = {
	    "server", "clients", "memory", "persistence", "stats", "replication",
	    "cpu", "module_list", "errorstats", "cluster", "keyspace", NULL };
	if (!defaults)
		defaults = default_sections;

	if (argc == 0) {
		/* In this case we know the dict is not gonna be modified, so we cache
		 * it as an optimization for a common case. */
		if (cached_default_info_sections)
			return cached_default_info_sections;
		cached_default_info_sections = dictCreate(&stringSetDictType);
		dictExpand(cached_default_info_sections, 16);
		addInfoSectionsToDict(cached_default_info_sections, defaults);
		return cached_default_info_sections;
	}

	dict *section_dict = dictCreate(&stringSetDictType);
	dictExpand(section_dict, min(argc, 16));
	for (int i = 0; i < argc; i++) {
		if (!strcasecmp(argv[i]->ptr, "default")) {
			addInfoSectionsToDict(section_dict, defaults);
		}
		else if (!strcasecmp(argv[i]->ptr, "all")) {
			if (out_all) *out_all = 1;
		}
		else if (!strcasecmp(argv[i]->ptr, "everything")) {
			if (out_everything) *out_everything = 1;
			if (out_all) *out_all = 1;
		}
		else {
			sds section = sdsnew(argv[i]->ptr);
			if (dictAdd(section_dict, section, NULL) != DICT_OK)
				sdsfree(section);
		}
	}
	return section_dict;
}

/* Create the string returned by the INFO command. This is decoupled
 * by the INFO command itself as we need to report the same information
 * on memory corruption problems. */
sds genRedisInfoString(dict *section_dict, int all_sections, int everything) {
	sds info = sdsempty();
	time_t uptime = server.unixtime - server.stat_starttime;
	int j;
	int sections = 0;
	if (everything) all_sections = 1;

	/* Server */
	if (all_sections || (dictFind(section_dict, "server") != NULL)) {
		static int call_uname = 1;
		static struct utsname name;
		char *mode;
		char *supervised;

		mode = "standalone";
		supervised = "no";

		if (sections++) info = sdscat(info, "\r\n");

		if (call_uname) {
			/* Uname can be slow and is always the same output. Cache it. */
			uname(&name);
			call_uname = 0;
		}

		unsigned int lruclock;
		atomicGet(server.lruclock, lruclock);
		info = sdscatfmt(info,
			"# Server\r\n"
			"redis_version:%s\r\n"
			"redis_git_sha1:%s\r\n"
			"redis_git_dirty:%i\r\n"
			"redis_build_id:%s\r\n"
			"redis_mode:%s\r\n"
			"os:%s %s %s\r\n"
			"arch_bits:%i\r\n"
			"monotonic_clock:%s\r\n"
			"multiplexing_api:%s\r\n"
			"atomicvar_api:%s\r\n"
			"gcc_version:%i.%i.%i\r\n"
			"process_id:%I\r\n"
			"process_supervised:%s\r\n"
			"run_id:%s\r\n"
			"tcp_port:%i\r\n"
			"server_time_usec:%I\r\n"
			"uptime_in_seconds:%I\r\n"
			"uptime_in_days:%I\r\n"
			"hz:%i\r\n"
			"configured_hz:%i\r\n"
			"lru_clock:%u\r\n"
			"executable:%s\r\n"
			"config_file:%s\r\n"
			"io_threads_active:%i\r\n",
			"",
			"",
			strtol(0, NULL, 10) > 0,
			"",
			mode,
			name.sysname, name.release, name.machine,
			server.arch_bits,
			monotonicInfoString(),
			aeGetApiName(),
			REDIS_ATOMIC_API,
#ifdef __GNUC__
			__GNUC__, __GNUC_MINOR__, __GNUC_PATCHLEVEL__,
#else
			0, 0, 0,
#endif
			(int64_t)getpid(),
			supervised,
			server.runid,
			server.port ? server.port : server.tls_port,
			(int64_t)server.ustime,
			(int64_t)uptime,
			(int64_t)(uptime / (3600 * 24)),
			server.hz,
			server.config_hz,
			lruclock,
			server.executable ? server.executable : "",
			server.configfile ? server.configfile : "",
			server.io_threads_active);
	}

	/* Clients */
	if (all_sections || (dictFind(section_dict, "clients") != NULL)) {
		size_t maxin, maxout;
		getExpansiveClientsInfo(&maxin, &maxout);
		if (sections++) info = sdscat(info, "\r\n");
		info = sdscatprintf(info,
			"# Clients\r\n"
			"connected_clients:%lu\r\n"
			"cluster_connections:%lu\r\n"
			"maxclients:%u\r\n"
			"client_recent_max_input_buffer:%zu\r\n"
			"client_recent_max_output_buffer:%zu\r\n"
			"blocked_clients:%d\r\n"
			"tracking_clients:%d\r\n"
			"clients_in_timeout_table:%llu\r\n",
			listLength(server.clients) - listLength(server.slaves),
			0,
			server.maxclients,
			maxin, maxout,
			server.blocked_clients,
			server.tracking_clients,
			(unsigned long long) raxSize(server.clients_timeout_table));
	}

	/* Memory */
	if (all_sections || (dictFind(section_dict, "memory") != NULL)) {
		char hmem[64];
		char peak_hmem[64];
		char total_system_hmem[64];
		char used_memory_rss_hmem[64];
		char maxmemory_hmem[64];
		size_t zmalloc_used = zmalloc_used_memory();
		size_t total_system_mem = server.system_memory_size;
		const char *evict_policy = evictPolicyToString();
		struct redisMemOverhead *mh = getMemoryOverheadData();

		/* Peak memory is updated from time to time by serverCron() so it
		 * may happen that the instantaneous value is slightly bigger than
		 * the peak value. This may confuse users, so we update the peak
		 * if found smaller than the current memory usage. */
		if (zmalloc_used > server.stat_peak_memory)
			server.stat_peak_memory = zmalloc_used;

		bytesToHuman(hmem, zmalloc_used);
		bytesToHuman(peak_hmem, server.stat_peak_memory);
		bytesToHuman(total_system_hmem, total_system_mem);
		bytesToHuman(used_memory_rss_hmem, server.cron_malloc_stats.process_rss);
		bytesToHuman(maxmemory_hmem, server.maxmemory);

		if (sections++) info = sdscat(info, "\r\n");
		info = sdscatprintf(info,
			"# Memory\r\n"
			"used_memory:%zu\r\n"
			"used_memory_human:%s\r\n"
			"used_memory_rss:%zu\r\n"
			"used_memory_rss_human:%s\r\n"
			"used_memory_peak:%zu\r\n"
			"used_memory_peak_human:%s\r\n"
			"used_memory_peak_perc:%.2f%%\r\n"
			"used_memory_overhead:%zu\r\n"
			"used_memory_startup:%zu\r\n"
			"used_memory_dataset:%zu\r\n"
			"used_memory_dataset_perc:%.2f%%\r\n"
			"allocator_allocated:%zu\r\n"
			"allocator_active:%zu\r\n"
			"allocator_resident:%zu\r\n"
			"total_system_memory:%lu\r\n"
			"total_system_memory_human:%s\r\n"
			"used_memory_lua:%lld\r\n" /* deprecated, renamed to used_memory_vm_eval */
			"used_memory_vm_eval:%lld\r\n"
			"used_memory_lua_human:%s\r\n" /* deprecated */
			"used_memory_scripts_eval:%lld\r\n"
			"number_of_cached_scripts:%lu\r\n"
			"number_of_functions:%lu\r\n"
			"number_of_libraries:%lu\r\n"
			"used_memory_vm_functions:%lld\r\n"
			"used_memory_vm_total:%lld\r\n"
			"used_memory_vm_total_human:%s\r\n"
			"used_memory_functions:%lld\r\n"
			"used_memory_scripts:%lld\r\n"
			"used_memory_scripts_human:%s\r\n"
			"maxmemory:%lld\r\n"
			"maxmemory_human:%s\r\n"
			"maxmemory_policy:%s\r\n"
			"allocator_frag_ratio:%.2f\r\n"
			"allocator_frag_bytes:%zu\r\n"
			"allocator_rss_ratio:%.2f\r\n"
			"allocator_rss_bytes:%zd\r\n"
			"rss_overhead_ratio:%.2f\r\n"
			"rss_overhead_bytes:%zd\r\n"
			"mem_fragmentation_ratio:%.2f\r\n"
			"mem_fragmentation_bytes:%zd\r\n"
			"mem_not_counted_for_evict:%zu\r\n"
			"mem_replication_backlog:%zu\r\n"
			"mem_total_replication_buffers:%zu\r\n"
			"mem_clients_slaves:%zu\r\n"
			"mem_clients_normal:%zu\r\n"
			"mem_cluster_links:%zu\r\n"
			"mem_aof_buffer:%zu\r\n"
			"mem_allocator:%s\r\n"
			"active_defrag_running:%d\r\n",
			zmalloc_used,
			hmem,
			server.cron_malloc_stats.process_rss,
			used_memory_rss_hmem,
			server.stat_peak_memory,
			peak_hmem,
			mh->peak_perc,
			mh->overhead_total,
			mh->startup_allocated,
			mh->dataset,
			mh->dataset_perc,
			server.cron_malloc_stats.allocator_allocated,
			server.cron_malloc_stats.allocator_active,
			server.cron_malloc_stats.allocator_resident,
			(unsigned long)total_system_mem,
			total_system_hmem,
			server.maxmemory,
			maxmemory_hmem,
			evict_policy,
			mh->allocator_frag,
			mh->allocator_frag_bytes,
			mh->allocator_rss,
			mh->allocator_rss_bytes,
			mh->rss_extra,
			mh->rss_extra_bytes,
			mh->total_frag,       /* This is the total RSS overhead, including
						 fragmentation, but not just it. This field
						 (and the next one) is named like that just
						 for backward compatibility. */
			mh->total_frag_bytes,
			freeMemoryGetNotCountedMemory(),
			mh->clients_slaves,
			mh->clients_normal,
			mh->cluster_links,
			mh->aof_buffer,
			ZMALLOC_LIB,
			server.active_defrag_running
		);
		freeMemoryOverheadData(mh);
	}

	/* Persistence */
	if (all_sections || (dictFind(section_dict, "persistence") != NULL)) {
		if (sections++) info = sdscat(info, "\r\n");
		double fork_perc = 0;
		if (server.stat_module_progress) {
			fork_perc = server.stat_module_progress * 100;
		}
		else if (server.stat_current_save_keys_total) {
			fork_perc = ((double)server.stat_current_save_keys_processed / server.stat_current_save_keys_total) * 100;
		}
		info = sdscatprintf(info,
			"# Persistence\r\n"
			"loading:%d\r\n"
			"async_loading:%d\r\n"
			"current_cow_peak:%zu\r\n"
			"current_cow_size:%zu\r\n"
			"current_cow_size_age:%lu\r\n"
			"current_fork_perc:%.2f\r\n"
			"current_save_keys_processed:%zu\r\n"
			"current_save_keys_total:%zu\r\n"
			"rdb_changes_since_last_save:%lld\r\n"
			"rdb_bgsave_in_progress:%d\r\n"
			"rdb_last_save_time:%jd\r\n"
			"rdb_last_bgsave_status:%s\r\n"
			"rdb_last_bgsave_time_sec:%jd\r\n"
			"rdb_current_bgsave_time_sec:%jd\r\n"
			"rdb_saves:%lld\r\n"
			"rdb_last_cow_size:%zu\r\n"
			"rdb_last_load_keys_expired:%lld\r\n"
			"rdb_last_load_keys_loaded:%lld\r\n"
			"aof_enabled:%d\r\n"
			"aof_rewrite_in_progress:%d\r\n"
			"aof_rewrite_scheduled:%d\r\n"
			"aof_last_rewrite_time_sec:%jd\r\n"
			"aof_current_rewrite_time_sec:%jd\r\n"
			"aof_last_bgrewrite_status:%s\r\n"
			"aof_rewrites:%lld\r\n"
			"aof_rewrites_consecutive_failures:%lld\r\n"
			"aof_last_write_status:%s\r\n"
			"aof_last_cow_size:%zu\r\n"
			"module_fork_in_progress:%d\r\n"
			"module_fork_last_cow_size:%zu\r\n",
			(int)(server.loading && !server.async_loading),
			(int)server.async_loading,
			server.stat_current_cow_peak,
			server.stat_current_cow_bytes,
			server.stat_current_cow_updated ? (unsigned long)elapsedMs(server.stat_current_cow_updated) / 1000 : 0,
			fork_perc,
			server.stat_current_save_keys_processed,
			server.stat_current_save_keys_total,
			server.stat_module_cow_bytes);

		if (server.loading) {
			double perc = 0;
			time_t eta, elapsed;
			off_t remaining_bytes = 1;

			if (server.loading_total_bytes) {
				perc = ((double)server.loading_loaded_bytes / server.loading_total_bytes) * 100;
				remaining_bytes = server.loading_total_bytes - server.loading_loaded_bytes;
			}
			else if (server.loading_rdb_used_mem) {
				perc = ((double)server.loading_loaded_bytes / server.loading_rdb_used_mem) * 100;
				remaining_bytes = server.loading_rdb_used_mem - server.loading_loaded_bytes;
				/* used mem is only a (bad) estimation of the rdb file size, avoid going over 100% */
				if (perc > 99.99) perc = 99.99;
				if (remaining_bytes < 1) remaining_bytes = 1;
			}

			elapsed = time(NULL) - server.loading_start_time;
			if (elapsed == 0) {
				eta = 1; /* A fake 1 second figure if we don't have
					    enough info */
			}
			else {
				eta = (elapsed*remaining_bytes) / (server.loading_loaded_bytes + 1);
			}

			info = sdscatprintf(info,
				"loading_start_time:%jd\r\n"
				"loading_total_bytes:%llu\r\n"
				"loading_rdb_used_mem:%llu\r\n"
				"loading_loaded_bytes:%llu\r\n"
				"loading_loaded_perc:%.2f\r\n"
				"loading_eta_seconds:%jd\r\n",
				(intmax_t)server.loading_start_time,
				(unsigned long long) server.loading_total_bytes,
				(unsigned long long) server.loading_rdb_used_mem,
				(unsigned long long) server.loading_loaded_bytes,
				perc,
				(intmax_t)eta
			);
		}
	}

	/* Stats */
	if (all_sections || (dictFind(section_dict, "stats") != NULL)) {
		long long stat_total_reads_processed, stat_total_writes_processed;
		long long stat_net_input_bytes, stat_net_output_bytes;
		long long stat_net_repl_input_bytes, stat_net_repl_output_bytes;
		long long current_eviction_exceeded_time = server.stat_last_eviction_exceeded_time ?
			(long long)elapsedUs(server.stat_last_eviction_exceeded_time) : 0;
		long long current_active_defrag_time = server.stat_last_active_defrag_time ?
			(long long)elapsedUs(server.stat_last_active_defrag_time) : 0;
		atomicGet(server.stat_total_reads_processed, stat_total_reads_processed);
		atomicGet(server.stat_total_writes_processed, stat_total_writes_processed);
		atomicGet(server.stat_net_input_bytes, stat_net_input_bytes);
		atomicGet(server.stat_net_output_bytes, stat_net_output_bytes);
		atomicGet(server.stat_net_repl_input_bytes, stat_net_repl_input_bytes);
		atomicGet(server.stat_net_repl_output_bytes, stat_net_repl_output_bytes);

		if (sections++) info = sdscat(info, "\r\n");
		info = sdscatprintf(info,
			"# Stats\r\n"
			"total_connections_received:%lld\r\n"
			"total_commands_processed:%lld\r\n"
			"instantaneous_ops_per_sec:%lld\r\n"
			"total_net_input_bytes:%lld\r\n"
			"total_net_output_bytes:%lld\r\n"
			"total_net_repl_input_bytes:%lld\r\n"
			"total_net_repl_output_bytes:%lld\r\n"
			"instantaneous_input_kbps:%.2f\r\n"
			"instantaneous_output_kbps:%.2f\r\n"
			"instantaneous_input_repl_kbps:%.2f\r\n"
			"instantaneous_output_repl_kbps:%.2f\r\n"
			"rejected_connections:%lld\r\n"
			"sync_full:%lld\r\n"
			"sync_partial_ok:%lld\r\n"
			"sync_partial_err:%lld\r\n"
			"expired_keys:%lld\r\n"
			"expired_stale_perc:%.2f\r\n"
			"expired_time_cap_reached_count:%lld\r\n"
			"expire_cycle_cpu_milliseconds:%lld\r\n"
			"evicted_keys:%lld\r\n"
			"evicted_clients:%lld\r\n"
			"total_eviction_exceeded_time:%lld\r\n"
			"current_eviction_exceeded_time:%lld\r\n"
			"keyspace_hits:%lld\r\n"
			"keyspace_misses:%lld\r\n"
			"pubsub_channels:%ld\r\n"
			"pubsub_patterns:%lu\r\n"
			"pubsubshard_channels:%lu\r\n"
			"latest_fork_usec:%lld\r\n"
			"total_forks:%lld\r\n"
			"migrate_cached_sockets:%ld\r\n"
			"slave_expires_tracked_keys:%zu\r\n"
			"active_defrag_hits:%lld\r\n"
			"active_defrag_misses:%lld\r\n"
			"active_defrag_key_hits:%lld\r\n"
			"active_defrag_key_misses:%lld\r\n"
			"total_active_defrag_time:%lld\r\n"
			"current_active_defrag_time:%lld\r\n"
			"tracking_total_keys:%lld\r\n"
			"tracking_total_items:%lld\r\n"
			"tracking_total_prefixes:%lld\r\n"
			"unexpected_error_replies:%lld\r\n"
			"total_error_replies:%lld\r\n"
			"dump_payload_sanitizations:%lld\r\n"
			"total_reads_processed:%lld\r\n"
			"total_writes_processed:%lld\r\n"
			"io_threaded_reads_processed:%lld\r\n"
			"io_threaded_writes_processed:%lld\r\n"
			"reply_buffer_shrinks:%lld\r\n"
			"reply_buffer_expands:%lld\r\n",
			server.stat_numconnections,
			server.stat_numcommands,
			getInstantaneousMetric(STATS_METRIC_COMMAND),
			stat_net_input_bytes + stat_net_repl_input_bytes,
			stat_net_output_bytes + stat_net_repl_output_bytes,
			stat_net_repl_input_bytes,
			stat_net_repl_output_bytes,
			(float)getInstantaneousMetric(STATS_METRIC_NET_INPUT) / 1024,
			(float)getInstantaneousMetric(STATS_METRIC_NET_OUTPUT) / 1024,
			(float)getInstantaneousMetric(STATS_METRIC_NET_INPUT_REPLICATION) / 1024,
			(float)getInstantaneousMetric(STATS_METRIC_NET_OUTPUT_REPLICATION) / 1024,
			server.stat_rejected_conn,
			server.stat_sync_full,
			server.stat_sync_partial_ok,
			server.stat_sync_partial_err,
			server.stat_expiredkeys,
			server.stat_expired_stale_perc * 100,
			server.stat_expired_time_cap_reached_count,
			server.stat_expire_cycle_time_used / 1000,
			server.stat_evictedkeys,
			server.stat_evictedclients,
			(server.stat_total_eviction_exceeded_time + current_eviction_exceeded_time) / 1000,
			current_eviction_exceeded_time / 1000,
			server.stat_keyspace_hits,
			server.stat_keyspace_misses,
			dictSize(server.pubsub_channels),
			dictSize(server.pubsub_patterns),
			dictSize(server.pubsubshard_channels),
			server.stat_fork_time,
			server.stat_total_forks,
			dictSize(server.migrate_cached_sockets),
			server.stat_active_defrag_hits,
			server.stat_active_defrag_misses,
			server.stat_active_defrag_key_hits,
			server.stat_active_defrag_key_misses,
			(server.stat_total_active_defrag_time + current_active_defrag_time) / 1000,
			current_active_defrag_time / 1000,
			(unsigned long long) trackingGetTotalKeys(),
			(unsigned long long) trackingGetTotalItems(),
			(unsigned long long) trackingGetTotalPrefixes(),
			server.stat_unexpected_error_replies,
			server.stat_total_error_replies,
			server.stat_dump_payload_sanitizations,
			stat_total_reads_processed,
			stat_total_writes_processed,
			server.stat_io_reads_processed,
			server.stat_io_writes_processed,
			server.stat_reply_buffer_shrinks,
			server.stat_reply_buffer_expands);
	}

	/* CPU */
	if (all_sections || (dictFind(section_dict, "cpu") != NULL)) {
		if (sections++) info = sdscat(info, "\r\n");

		struct rusage self_ru, c_ru;
		getrusage(RUSAGE_SELF, &self_ru);
		getrusage(RUSAGE_CHILDREN, &c_ru);
		info = sdscatprintf(info,
			"# CPU\r\n"
			"used_cpu_sys:%ld.%06ld\r\n"
			"used_cpu_user:%ld.%06ld\r\n"
			"used_cpu_sys_children:%ld.%06ld\r\n"
			"used_cpu_user_children:%ld.%06ld\r\n",
			(long)self_ru.ru_stime.tv_sec, (long)self_ru.ru_stime.tv_usec,
			(long)self_ru.ru_utime.tv_sec, (long)self_ru.ru_utime.tv_usec,
			(long)c_ru.ru_stime.tv_sec, (long)c_ru.ru_stime.tv_usec,
			(long)c_ru.ru_utime.tv_sec, (long)c_ru.ru_utime.tv_usec);
#ifdef RUSAGE_THREAD
		struct rusage m_ru;
		getrusage(RUSAGE_THREAD, &m_ru);
		info = sdscatprintf(info,
			"used_cpu_sys_main_thread:%ld.%06ld\r\n"
			"used_cpu_user_main_thread:%ld.%06ld\r\n",
			(long)m_ru.ru_stime.tv_sec, (long)m_ru.ru_stime.tv_usec,
			(long)m_ru.ru_utime.tv_sec, (long)m_ru.ru_utime.tv_usec);
#endif  /* RUSAGE_THREAD */
	}

	/* Modules */
	if (all_sections || (dictFind(section_dict, "module_list") != NULL) || (dictFind(section_dict, "modules") != NULL)) {
		if (sections++) info = sdscat(info, "\r\n");
		info = sdscatprintf(info, "# Modules\r\n");
		info = genModulesInfoString(info);
	}

	/* Command statistics */
	if (all_sections || (dictFind(section_dict, "commandstats") != NULL)) {
		if (sections++) info = sdscat(info, "\r\n");
		info = sdscatprintf(info, "# Commandstats\r\n");
		info = genRedisInfoStringCommandStats(info, server.commands);
	}

	/* Error statistics */
	if (all_sections || (dictFind(section_dict, "errorstats") != NULL)) {
		if (sections++) info = sdscat(info, "\r\n");
		info = sdscat(info, "# Errorstats\r\n");
		raxIterator ri;
		raxStart(&ri, server.errors);
		raxSeek(&ri, "^", NULL, 0);
		struct redisError *e;
		while (raxNext(&ri)) {
			char *tmpsafe;
			e = (struct redisError *) ri.data;
			info = sdscatprintf(info,
				"errorstat_%.*s:count=%lld\r\n",
				(int)ri.key_len, getSafeInfoString((char *)ri.key, ri.key_len, &tmpsafe), e->count);
			if (tmpsafe != NULL) zfree(tmpsafe);
		}
		raxStop(&ri);
	}

	/* Latency by percentile distribution per command */
	if (all_sections || (dictFind(section_dict, "latencystats") != NULL)) {
		if (sections++) info = sdscat(info, "\r\n");
		info = sdscatprintf(info, "# Latencystats\r\n");
		if (server.latency_tracking_enabled) {
			info = genRedisInfoStringLatencyStats(info, server.commands);
		}
	}

	return info;
}

/* INFO [<section> [<section> ...]] */
void infoCommand(client *c) {
	int all_sections = 0;
	int everything = 0;
	dict *sections_dict = genInfoSectionDict(c->argv + 1, c->argc - 1, NULL, &all_sections, &everything);
	sds info = genRedisInfoString(sections_dict, all_sections, everything);
	addReplyVerbatim(c, info, sdslen(info), "txt");
	sdsfree(info);
	releaseInfoSectionDict(sections_dict);
	return;
}

void monitorCommand(client *c) {
	if (c->flags & CLIENT_DENY_BLOCKING) {
		/**
		 * A client that has CLIENT_DENY_BLOCKING flag on
		 * expects a reply per command and so can't execute MONITOR. */
		addReplyError(c, "MONITOR isn't allowed for DENY BLOCKING client");
		return;
	}

	/* ignore MONITOR if already slave or in monitor mode */
	if (c->flags & CLIENT_SLAVE) return;

	c->flags |= (CLIENT_SLAVE | CLIENT_MONITOR);
	listAddNodeTail(server.monitors, c);
	addReply(c, shared.ok);
}

/* =================================== Main! ================================ */

int checkIgnoreWarning(const char *warning) {
	int argc, j;
	sds *argv = sdssplitargs(server.ignore_warnings, &argc);
	if (argv == NULL)
		return 0;

	for (j = 0; j < argc; j++) {
		char *flag = argv[j];
		if (!strcasecmp(flag, warning))
			break;
	}
	sdsfreesplitres(argv, argc);
	return j < argc;
}

#ifdef __linux__
#include <sys/prctl.h>
/* since linux-3.5, kernel supports to set the state of the "THP disable" flag
 * for the calling thread. PR_SET_THP_DISABLE is defined in linux/prctl.h */
static int THPDisable(void) {
	int ret = -EINVAL;

	if (!server.disable_thp)
		return ret;

#ifdef PR_SET_THP_DISABLE
	ret = prctl(PR_SET_THP_DISABLE, 1, 0, 0, 0);
#endif

	return ret;
}

void linuxMemoryWarnings(void) {
	sds err_msg = NULL;
	if (checkOvercommit(&err_msg) < 0) {
		serverLog(LL_WARNING, "WARNING %s", err_msg);
		sdsfree(err_msg);
	}
	if (checkTHPEnabled(&err_msg) < 0) {
		server.thp_enabled = 1;
		if (THPDisable() == 0) {
			server.thp_enabled = 0;
		}
		else {
			serverLog(LL_WARNING, "WARNING %s", err_msg);
		}
		sdsfree(err_msg);
	}
}
#endif /* __linux__ */

void createPidFile(void) {
	/* If pidfile requested, but no pidfile defined, use
	 * default pidfile path */
	if (!server.pidfile) server.pidfile = zstrdup(CONFIG_DEFAULT_PID_FILE);

	/* Try to write the pid file in a best-effort way. */
	FILE *fp = fopen(server.pidfile, "w");
	if (fp) {
		fprintf(fp, "%d\n", (int)getpid());
		fclose(fp);
	}
}

void daemonize(void) {
	int fd;

	if (fork() != 0) exit(0); /* parent exits */
	setsid(); /* create a new session */

	/* Every output goes to /dev/null. If Redis is daemonized but
	 * the 'logfile' is set to 'stdout' in the configuration file
	 * it will not log at all. */
	if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
		dup2(fd, STDIN_FILENO);
		dup2(fd, STDOUT_FILENO);
		dup2(fd, STDERR_FILENO);
		if (fd > STDERR_FILENO) close(fd);
	}
}

int changeBindAddr(void) {
	/* Close old TCP and TLS servers */
	closeSocketListeners(&server.ipfd);
	closeSocketListeners(&server.tlsfd);

	/* Bind to the new port */
	if ((server.port != 0 && listenToPort(server.port, &server.ipfd) != C_OK) ||
		(server.tls_port != 0 && listenToPort(server.tls_port, &server.tlsfd) != C_OK)) {
		serverLog(LL_WARNING, "Failed to bind");

		closeSocketListeners(&server.ipfd);
		closeSocketListeners(&server.tlsfd);
		return C_ERR;
	}

	/* Create TCP and TLS event handlers */
	if (createSocketAcceptHandler(&server.ipfd, acceptTcpHandler) != C_OK) {
		serverPanic("Unrecoverable error creating TCP socket accept handler.");
	}
	if (createSocketAcceptHandler(&server.tlsfd, acceptTLSHandler) != C_OK) {
		serverPanic("Unrecoverable error creating TLS socket accept handler.");
	}

	if (server.set_proc_title) redisSetProcTitle(NULL);

	return C_OK;
}

int changeListenPort(int port, socketFds *sfd, aeFileProc *accept_handler) {
	socketFds new_sfd = { {0} };

	/* Close old servers */
	closeSocketListeners(sfd);

	/* Just close the server if port disabled */
	if (port == 0) {
		if (server.set_proc_title) redisSetProcTitle(NULL);
		return C_OK;
	}

	/* Bind to the new port */
	if (listenToPort(port, &new_sfd) != C_OK) {
		return C_ERR;
	}

	/* Create event handlers */
	if (createSocketAcceptHandler(&new_sfd, accept_handler) != C_OK) {
		closeSocketListeners(&new_sfd);
		return C_ERR;
	}

	/* Copy new descriptors */
	sfd->count = new_sfd.count;
	memcpy(sfd->fd, new_sfd.fd, sizeof(new_sfd.fd));

	if (server.set_proc_title) redisSetProcTitle(NULL);

	return C_OK;
}


/* Try to release pages back to the OS directly (bypassing the allocator),
 * in an effort to decrease CoW during fork. For small allocations, we can't
 * release any full page, so in an effort to avoid getting the size of the
 * allocation from the allocator (malloc_size) when we already know it's small,
 * we check the size_hint. If the size is not already known, passing a size_hint
 * of 0 will lead the checking the real size of the allocation.
 * Also please note that the size may be not accurate, so in order to make this
 * solution effective, the judgement for releasing memory pages should not be
 * too strict. */
void dismissMemory(void* ptr, size_t size_hint) {
	if (ptr == NULL) return;

	/* madvise(MADV_DONTNEED) can not release pages if the size of memory
	 * is too small, we try to release only for the memory which the size
	 * is more than half of page size. */
	if (size_hint && size_hint <= server.page_size / 2) return;

	zmadvise_dontneed(ptr);
}

/* Dismiss big chunks of memory inside a client structure, see dismissMemory() */
void dismissClientMemory(client *c) {
	/* Dismiss client query buffer and static reply buffer. */
	dismissMemory(c->buf, c->buf_usable_size);
	dismissSds(c->querybuf);
	/* Dismiss argv array only if we estimate it contains a big buffer. */
	if (c->argc && c->argv_len_sum / c->argc >= server.page_size) {
		for (int i = 0; i < c->argc; i++) {
			dismissObject(c->argv[i], 0);
		}
	}
	if (c->argc) dismissMemory(c->argv, c->argc * sizeof(robj*));

	/* Dismiss the reply array only if the average buffer size is bigger
	 * than a page. */
	if (listLength(c->reply) &&
		c->reply_bytes / listLength(c->reply) >= server.page_size)
	{
		listIter li;
		listNode *ln;
		listRewind(c->reply, &li);
		while ((ln = listNext(&li))) {
			clientReplyBlock *bulk = listNodeValue(ln);
			/* Default bulk size is 16k, actually it has extra data, maybe it
			 * occupies 20k according to jemalloc bin size if using jemalloc. */
			if (bulk) dismissMemory(bulk, bulk->size);
		}
	}
}

/* In the child process, we don't need some buffers anymore, and these are
 * likely to change in the parent when there's heavy write traffic.
 * We dismiss them right away, to avoid CoW.
 * see dismissMemeory(). */
void dismissMemoryInChild(void) {
	/* madvise(MADV_DONTNEED) may not work if Transparent Huge Pages is enabled. */
	if (server.thp_enabled) return;

	/* Currently we use zmadvise_dontneed only when we use jemalloc with Linux.
	 * so we avoid these pointless loops when they're not going to do anything. */
#if defined(USE_JEMALLOC) && defined(__linux__)
	listIter li;
	listNode *ln;

	/* Dismiss replication buffer. We don't need to separately dismiss replication
	 * backlog and replica' output buffer, because they just reference the global
	 * replication buffer but don't cost real memory. */
	listRewind(server.repl_buffer_blocks, &li);
	while ((ln = listNext(&li))) {
		replBufBlock *o = listNodeValue(ln);
		dismissMemory(o, o->size);
	}

	/* Dismiss all clients memory. */
	listRewind(server.clients, &li);
	while ((ln = listNext(&li))) {
		client *c = listNodeValue(ln);
		dismissClientMemory(c);
	}
#endif
}

void memtest(size_t megabytes, int passes);


void redisOutOfMemoryHandler(size_t allocation_size) {
	serverLog(LL_WARNING, "Out Of Memory allocating %zu bytes!",
		allocation_size);
	serverPanic("Redis aborting for OUT OF MEMORY. Allocating %zu bytes!",
		allocation_size);
}

/* Callback for sdstemplate on proc-title-template. See redis.conf for
 * supported variables.
 */
static sds redisProcTitleGetVariable(const sds varname, void *arg)
{
	if (!strcmp(varname, "title")) {
		return sdsnew(arg);
	}
	else if (!strcmp(varname, "listen-addr")) {
		if (server.port || server.tls_port)
			return sdscatprintf(sdsempty(), "%s:%u",
				server.bindaddr_count ? server.bindaddr[0] : "*",
				server.port ? server.port : server.tls_port);
		else
			return sdscatprintf(sdsempty(), "unixsocket:%s", server.unixsocket);
	}
	else if (!strcmp(varname, "config-file")) {
		return sdsnew(server.configfile ? server.configfile : "-");
	}
	else if (!strcmp(varname, "port")) {
		return sdscatprintf(sdsempty(), "%u", server.port);
	}
	else if (!strcmp(varname, "tls-port")) {
		return sdscatprintf(sdsempty(), "%u", server.tls_port);
	}
	else if (!strcmp(varname, "unixsocket")) {
		return sdsnew(server.unixsocket);
	}
	else
		return NULL;    /* Unknown variable name */
}

/* Expand the specified proc-title-template string and return a newly
 * allocated sds, or NULL. */
static sds expandProcTitleTemplate(const char *template, const char *title) {
	sds res = sdstemplate(template, redisProcTitleGetVariable, (void *)title);
	if (!res)
		return NULL;
	return sdstrim(res, " ");
}
/* Validate the specified template, returns 1 if valid or 0 otherwise. */
int validateProcTitleTemplate(const char *template) {
	int ok = 1;
	sds res = expandProcTitleTemplate(template, "");
	if (!res)
		return 0;
	if (sdslen(res) == 0) ok = 0;
	sdsfree(res);
	return ok;
}

int redisSetProcTitle(char *title) {
#ifdef USE_SETPROCTITLE
	if (!title) title = server.exec_argv[0];
	sds proc_title = expandProcTitleTemplate(server.proc_title_template, title);
	if (!proc_title) return C_ERR;  /* Not likely, proc_title_template is validated */

	setproctitle("%s", proc_title);
	sdsfree(proc_title);
#else
	UNUSED(title);
#endif

	return C_OK;
}

void redisSetCpuAffinity(const char *cpulist) {
#ifdef USE_SETCPUAFFINITY
	setcpuaffinity(cpulist);
#else
	UNUSED(cpulist);
#endif
}


/* Attempt to set up upstart supervision. Returns 1 if successful. */
static int redisSupervisedUpstart(void) {
	const char *upstart_job = getenv("UPSTART_JOB");

	if (!upstart_job) {
		serverLog(LL_WARNING,
			"upstart supervision requested, but UPSTART_JOB not found!");
		return 0;
	}

	serverLog(LL_NOTICE, "supervised by upstart, will stop to signal readiness.");
	raise(SIGSTOP);
	unsetenv("UPSTART_JOB");
	return 1;
}

/* Attempt to set up systemd supervision. Returns 1 if successful. */
static int redisSupervisedSystemd(void) {
#ifndef HAVE_LIBSYSTEMD
	serverLog(LL_WARNING,
		"systemd supervision requested or auto-detected, but Redis is compiled without libsystemd support!");
	return 0;
#else
	if (redisCommunicateSystemd("STATUS=Redis is loading...\n") <= 0)
		return 0;
	serverLog(LL_NOTICE,
		"Supervised by systemd. Please make sure you set appropriate values for TimeoutStartSec and TimeoutStopSec in your service unit.");
	return 1;
#endif
}


int main(int argc, char **argv) {
	struct timeval tv;
	int j;
	char config_from_stdin = 0;

	/* We need to initialize our libraries, and the server configuration. */
#ifdef INIT_SETPROCTITLE_REPLACEMENT
	spt_init(argc, argv);
#endif
	setlocale(LC_COLLATE, "");
	tzset(); /* Populates 'timezone' global. */
	zmalloc_set_oom_handler(redisOutOfMemoryHandler);

	/* To achieve entropy, in case of containers, their time() and getpid() can
	 * be the same. But value of tv_usec is fast enough to make the difference */
	gettimeofday(&tv, NULL);
	srand(time(NULL) ^ getpid() ^ tv.tv_usec);
	srandom(time(NULL) ^ getpid() ^ tv.tv_usec);
	init_genrand64(((long long)tv.tv_sec * 1000000 + tv.tv_usec) ^ getpid());
	crc64_init();

	/* Store umask value. Because umask(2) only offers a set-and-get API we have
	 * to reset it and restore it back. We do this early to avoid a potential
	 * race condition with threads that could be creating files or directories.
	 */
	umask(server.umask = umask(0777));

	uint8_t hashseed[16];
	getRandomBytes(hashseed, sizeof(hashseed));
	dictSetHashFunctionSeed(hashseed);

	char *exec_name = strrchr(argv[0], '/');
	if (exec_name == NULL) exec_name = argv[0];
	initServerConfig();
	ACLInit(); /* The ACL subsystem must be initialized ASAP because the
		      basic networking code and client creation depends on it. */
	tlsInit();

	/* Store the executable path and arguments in a safe place in order
	 * to be able to restart the server later. */
	server.executable = getAbsolutePath(argv[0]);
	server.exec_argv = zmalloc(sizeof(char*)*(argc + 1));
	server.exec_argv[argc] = NULL;
	for (j = 0; j < argc; j++) server.exec_argv[j] = zstrdup(argv[j]);

	if (argc >= 2) {
		j = 1; /* First option to parse in argv[] */
		sds options = sdsempty();

		/* Handle special options --help and --version */
		if (strcmp(argv[1], "--help") == 0 ||
			strcmp(argv[1], "-h") == 0) usage();
		if (strcmp(argv[1], "--test-memory") == 0) {
			if (argc == 3) {
				memtest(atoi(argv[2]), 50);
				exit(0);
			}
			else {
				fprintf(stderr, "Please specify the amount of memory to test in megabytes.\n");
				fprintf(stderr, "Example: ./redis-server --test-memory 4096\n\n");
				exit(1);
			}
		} if (strcmp(argv[1], "--check-system") == 0) {
			exit(syscheck() ? 0 : 1);
		}
		/* Parse command line options
		 * Precedence wise, File, stdin, explicit options -- last config is the one that matters.
		 *
		 * First argument is the config file name? */
		if (argv[1][0] != '-') {
			/* Replace the config file in server.exec_argv with its absolute path. */
			server.configfile = getAbsolutePath(argv[1]);
			zfree(server.exec_argv[1]);
			server.exec_argv[1] = zstrdup(server.configfile);
			j = 2; // Skip this arg when parsing options
		}
		sds *argv_tmp;
		int argc_tmp;
		int handled_last_config_arg = 1;
		while (j < argc) {
			/* Either first or last argument - Should we read config from stdin? */
			if (argv[j][0] == '-' && argv[j][1] == '\0' && (j == 1 || j == argc - 1)) {
				config_from_stdin = 1;
			}
			/* All the other options are parsed and conceptually appended to the
			 * configuration file. For instance --port 6380 will generate the
			 * string "port 6380\n" to be parsed after the actual config file
			 * and stdin input are parsed (if they exist).
			 * Only consider that if the last config has at least one argument. */
			else if (handled_last_config_arg && argv[j][0] == '-' && argv[j][1] == '-') {
				/* Option name */
				if (sdslen(options)) options = sdscat(options, "\n");
				/* argv[j]+2 for removing the preceding `--` */
				options = sdscat(options, argv[j] + 2);
				options = sdscat(options, " ");

				argv_tmp = sdssplitargs(argv[j], &argc_tmp);
				if (argc_tmp == 1) {
					/* Means that we only have one option name, like --port or "--port " */
					handled_last_config_arg = 0;

					if ((j != argc - 1) && argv[j + 1][0] == '-' && argv[j + 1][1] == '-' &&
						!strcasecmp(argv[j], "--save"))
					{
						/* Special case: handle some things like `--save --config value`.
						 * In this case, if next argument starts with `--`, we will reset
						 * handled_last_config_arg flag and append an empty "" config value
						 * to the options, so it will become `--save "" --config value`.
						 * We are doing it to be compatible with pre 7.0 behavior (which we
						 * break it in #10660, 7.0.1), since there might be users who generate
						 * a command line from an array and when it's empty that's what they produce. */
						options = sdscat(options, "\"\"");
						handled_last_config_arg = 1;
					}
					else if ((j == argc - 1) && !strcasecmp(argv[j], "--save")) {
						/* Special case: when empty save is the last argument.
						 * In this case, we append an empty "" config value to the options,
						 * so it will become `--save ""` and will follow the same reset thing. */
						options = sdscat(options, "\"\"");
					}
					else if ((j != argc - 1) && argv[j + 1][0] == '-' && argv[j + 1][1] == '-' &&
						!strcasecmp(argv[j], "--sentinel"))
					{
						/* Special case: handle some things like `--sentinel --config value`.
						 * It is a pseudo config option with no value. In this case, if next
						 * argument starts with `--`, we will reset handled_last_config_arg flag.
						 * We are doing it to be compatible with pre 7.0 behavior (which we
						 * break it in #10660, 7.0.1). */
						options = sdscat(options, "");
						handled_last_config_arg = 1;
					}
					else if ((j == argc - 1) && !strcasecmp(argv[j], "--sentinel")) {
						/* Special case: when --sentinel is the last argument.
						 * It is a pseudo config option with no value. In this case, do nothing.
						 * We are doing it to be compatible with pre 7.0 behavior (which we
						 * break it in #10660, 7.0.1). */
						options = sdscat(options, "");
					}
				}
				else {
					/* Means that we are passing both config name and it's value in the same arg,
					 * like "--port 6380", so we need to reset handled_last_config_arg flag. */
					handled_last_config_arg = 1;
				}
				sdsfreesplitres(argv_tmp, argc_tmp);
			}
			else {
				/* Option argument */
				options = sdscatrepr(options, argv[j], strlen(argv[j]));
				options = sdscat(options, " ");
				handled_last_config_arg = 1;
			}
			j++;
		}

		loadServerConfig(server.configfile, config_from_stdin, options);
		sdsfree(options);
	}

	int background = server.daemonize;
	if (background) daemonize();

	serverLog(LL_WARNING, "oO0OoO0OoO0Oo Redis is starting oO0OoO0OoO0Oo");

	if (argc == 1) {
		serverLog(LL_WARNING, "Warning: no config file specified, using the default config. In order to specify a config file use %s /path/to/redis.conf", argv[0]);
	}
	else {
		serverLog(LL_WARNING, "Configuration loaded");
	}

	initServer();
	if (background || server.pidfile) createPidFile();
	if (server.set_proc_title) redisSetProcTitle(NULL);
	checkTcpBacklogSettings();

	if (!server.sentinel_mode) {
		/* Things not needed when running in Sentinel mode. */
		serverLog(LL_WARNING, "Server initialized");
#ifdef __linux__
		linuxMemoryWarnings();
		sds err_msg = NULL;
		if (checkXenClocksource(&err_msg) < 0) {
			serverLog(LL_WARNING, "WARNING %s", err_msg);
			sdsfree(err_msg);
		}
#if defined (__arm64__)
		int ret;
		if ((ret = checkLinuxMadvFreeForkBug(&err_msg)) <= 0) {
			if (ret < 0) {
				serverLog(LL_WARNING, "WARNING %s", err_msg);
				sdsfree(err_msg);
			}
			else
				serverLog(LL_WARNING, "Failed to test the kernel for a bug that could lead to data corruption during background save. "
					"Your system could be affected, please report this error.");
			if (!checkIgnoreWarning("ARM64-COW-BUG")) {
				serverLog(LL_WARNING, "Redis will now exit to prevent data corruption. "
					"Note that it is possible to suppress this warning by setting the following config: ignore-warnings ARM64-COW-BUG");
				exit(1);
			}
		}
#endif /* __arm64__ */
#endif /* __linux__ */
		ACLLoadUsersAtStartup();
		InitServerLast();

		if (server.ipfd.count > 0 || server.tlsfd.count > 0)
			serverLog(LL_NOTICE, "Ready to accept connections");
		if (server.sofd > 0)
			serverLog(LL_NOTICE, "The server is now ready to accept connections at %s", server.unixsocket);
	}
	else {
		ACLLoadUsersAtStartup();
		InitServerLast();
	}

	/* Warning the user about suspicious maxmemory setting. */
	if (server.maxmemory > 0 && server.maxmemory < 1024 * 1024) {
		serverLog(LL_WARNING, "WARNING: You specified a maxmemory value that is less than 1MB (current value is %llu bytes). Are you sure this is what you really want?", server.maxmemory);
	}

	redisSetCpuAffinity(server.server_cpulist);
	setOOMScoreAdj(-1);

	aeMain(server.el);
	aeDeleteEventLoop(server.el);
	return 0;
}

/* The End */
