// SPDX-License-Identifier: BSD-3-Clause

#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>

#include "consumer.h"
#include "ring_buffer.h"
#include "packet.h"
#include "utils.h"

void consumer_thread(so_consumer_ctx_t *ctx)
{
	int fd = open(ctx->out_filename, O_WRONLY | O_CREAT | O_APPEND, 0644);

	while (1)
	{
		so_packet_t packet;
		int result = ring_buffer_dequeue(ctx->producer_rb, &packet, PKT_SZ);
		if (result < 0)
			break;
		// pthread_mutex_lock(&ctx->timestamp_mutex);

		so_action_t action = process_packet(&packet);
		unsigned long hash = packet_hash(&packet);
		unsigned long time = packet.hdr.timestamp;

		// pthread_mutex_unlock(&ctx->timestamp_mutex);

		pthread_mutex_lock(&ctx->log_mutex);
		char buffer[PKT_SZ];
		int len = snprintf(buffer, sizeof(buffer), "%s %016lx %lu\n", RES_TO_STR(action), hash, time);
		write(fd, buffer, len);
		// dprintf(fd, "%s %016lx %lu\n", RES_TO_STR(action), hash, time);
		pthread_mutex_unlock(&ctx->log_mutex);
	}
	close(fd);
}

int create_consumers(pthread_t *tids, int num_consumers, struct so_ring_buffer_t *rb, const char *out_filename)
{
	for (int i = 0; i < num_consumers; i++)
	{
		so_consumer_ctx_t *ctx = malloc(sizeof(so_consumer_ctx_t));
		ctx->out_filename = out_filename;

		ctx->producer_rb = rb;
		pthread_mutex_init(&ctx->log_mutex, NULL);
		// pthread_mutex_init(&ctx->timestamp_mutex, NULL);

		int result = pthread_create(&tids[i], NULL, consumer_thread, ctx);
	}

	return num_consumers;
}
