/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef __SO_CONSUMER_H__
#define __SO_CONSUMER_H__

#include "ring_buffer.h"
#include "packet.h"

typedef struct
{
	so_action_t *action;
	unsigned long *hash;
	unsigned long *timestamp;
	size_t size;
	size_t capacity;
} min_heap_t;

typedef struct so_consumer_ctx_t
{
	struct so_ring_buffer_t *producer_rb;
	min_heap_t *heap;

	pthread_mutex_t log_mutex;
	pthread_mutex_t heap_mutex;
	pthread_cond_t cond;

	const char *out_filename;
	int finished_consumers;
	int num_consumers;
} so_consumer_ctx_t;

int create_consumers(pthread_t *tids, int num_consumers, so_ring_buffer_t *rb, const char *out_filename);

#endif /* __SO_CONSUMER_H__ */
