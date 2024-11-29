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
	so_packet_t packet;
	int fd = open(ctx->out_filename, O_WRONLY | O_CREAT | O_APPEND, 0644);

	while (ring_buffer_dequeue(ctx->producer_rb, &packet, PKT_SZ) != -1)
	{
		so_action_t action = process_packet(&packet);
		unsigned long hash = packet_hash(&packet);
		unsigned long time = packet.hdr.timestamp;

		pthread_mutex_lock(&ctx->log_mutex);

		char buffer[256];
		int len = snprintf(buffer, 256, "%s %016lx %lu\n", RES_TO_STR(action), hash, time);
		write(fd, buffer, len);

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

		pthread_create(&tids[i], NULL, consumer_thread, ctx);
	}
	return num_consumers;
}
