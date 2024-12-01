/* SPDX-License-Identifier: BSD-3-Clause */

#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>

#include "consumer.h"
#include "ring_buffer.h"
#include "packet.h"
#include "utils.h"

void heap_push(min_heap_t *heap, so_action_t action, unsigned long hash, unsigned long timestamp)
{
    if (heap->size == heap->capacity)
    {
        heap->capacity = heap->capacity == 0 ? 1 : heap->capacity * 10;
        heap->action = realloc(heap->action, heap->capacity * sizeof(so_action_t));
        heap->hash = realloc(heap->hash, heap->capacity * sizeof(unsigned long));
        heap->timestamp = realloc(heap->timestamp, heap->capacity * sizeof(unsigned long));
    }

    size_t i = heap->size++;
    while (i > 0)
    {
        size_t parent = (i - 1) / 2;
        if (heap->timestamp[parent] <= timestamp)
            break;

        heap->action[i] = heap->action[parent];
        heap->hash[i] = heap->hash[parent];
        heap->timestamp[i] = heap->timestamp[parent];
        i = parent;
    }
    heap->action[i] = action;
    heap->hash[i] = hash;
    heap->timestamp[i] = timestamp;
}

void heap_pop(min_heap_t *heap, so_action_t *action, unsigned long *hash, unsigned long *timestamp)
{
    *action = heap->action[0];
    *hash = heap->hash[0];
    *timestamp = heap->timestamp[0];

    so_action_t last_action = heap->action[--heap->size];
    unsigned long last_hash = heap->hash[heap->size];
    unsigned long last_timestamp = heap->timestamp[heap->size];

    size_t i = 0;
    while (2 * i + 1 < heap->size)
    {
        size_t left = 2 * i + 1;
        size_t right = 2 * i + 2;
        size_t smallest = left;

        if (right < heap->size && heap->timestamp[right] < heap->timestamp[left])
            smallest = right;

        if (last_timestamp <= heap->timestamp[smallest])
            break;

        heap->action[i] = heap->action[smallest];
        heap->hash[i] = heap->hash[smallest];
        heap->timestamp[i] = heap->timestamp[smallest];
        i = smallest;
    }

    heap->action[i] = last_action;
    heap->hash[i] = last_hash;
    heap->timestamp[i] = last_timestamp;
}

void heap_destroy(min_heap_t *heap)
{
    free(heap->action);
    free(heap->hash);
    free(heap->timestamp);
}

void write_sorted_log(so_consumer_ctx_t *ctx)
{
    int fd = open(ctx->out_filename, O_WRONLY | O_CREAT | O_APPEND, 0644);
    so_action_t action;
    unsigned long hash, timestamp;

    pthread_mutex_lock(&ctx->heap_mutex);

    while (ctx->finished_consumers < ctx->num_consumers)
        pthread_cond_wait(&ctx->cond, &ctx->heap_mutex);

    while (ctx->heap->size > 0)
    {
        heap_pop(ctx->heap, &action, &hash, &timestamp);

        // pthread_mutex_lock(&ctx->log_mutex);

        char buffer[256];
        int len = snprintf(buffer, 256, "%s %016lx %lu\n", RES_TO_STR(action), hash, timestamp);
        write(fd, buffer, len);

        // pthread_mutex_unlock(&ctx->log_mutex);
    }
    pthread_mutex_unlock(&ctx->heap_mutex);
    close(fd);
}

void consumer_thread(so_consumer_ctx_t *ctx)
{
    so_packet_t pkt;

    while (ring_buffer_dequeue(ctx->producer_rb, &pkt, PKT_SZ) != -1)
    {
        so_action_t action = process_packet(&pkt);
        unsigned long hash = packet_hash(&pkt);
        unsigned long time = pkt.hdr.timestamp;

        pthread_mutex_lock(&ctx->heap_mutex);
        heap_push(ctx->heap, action, hash, time);
        pthread_mutex_unlock(&ctx->heap_mutex);
    }
    ctx->finished_consumers++;
    if (ctx->finished_consumers == ctx->num_consumers)
        pthread_cond_signal(&ctx->cond);
    write_sorted_log(ctx);
}

int create_consumers(pthread_t *tids, int num_consumers, struct so_ring_buffer_t *rb, const char *out_filename)
{
    min_heap_t *heap = malloc(sizeof(min_heap_t));
    heap->action = NULL;
    heap->hash = NULL;
    heap->timestamp = NULL;
    heap->size = 0;
    heap->capacity = 0;

    so_consumer_ctx_t *ctx = malloc(sizeof(so_consumer_ctx_t));
    ctx->out_filename = out_filename;
    ctx->producer_rb = rb;
    ctx->heap = heap;
    ctx->finished_consumers = 0;
    ctx->num_consumers = num_consumers;

    pthread_mutex_init(&ctx->log_mutex, NULL);
    pthread_mutex_init(&ctx->heap_mutex, NULL);
    pthread_cond_init(&ctx->cond, NULL);

    for (int i = 0; i < num_consumers; i++)
        pthread_create(&tids[i], NULL, consumer_thread, ctx);
    return num_consumers;
}