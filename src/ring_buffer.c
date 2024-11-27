// SPDX-License-Identifier: BSD-3-Clause

#include <pthread.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include "ring_buffer.h"

int ring_buffer_init(so_ring_buffer_t *ring, size_t cap)
{
    if (ring == NULL || cap == 0)
        return -1;

    ring->data = malloc(cap);
    ring->cap = cap;
    ring->write_pos = ring->read_pos = 0;
    ring->len = 0;

    pthread_mutex_init(&ring->mutex, NULL);

    return 0;
}

ssize_t ring_buffer_enqueue(so_ring_buffer_t *ring, void *data, size_t size)
{
    pthread_mutex_lock(&ring->mutex);

    memcpy(ring->data + ring->write_pos, data, size);

    ring->write_pos = ring->write_pos + size;
    ring->len += size;

    pthread_mutex_unlock(&ring->mutex);

    return size;
}

ssize_t ring_buffer_dequeue(so_ring_buffer_t *ring, void *data, size_t size)
{
    if (ring->len == 0)
        return -1;

    pthread_mutex_lock(&ring->mutex);

    memcpy(data, ring->data + ring->read_pos, size);

    ring->read_pos = ring->read_pos + size;
    ring->len -= size;

    pthread_mutex_unlock(&ring->mutex);

    return size;
}

void ring_buffer_destroy(so_ring_buffer_t *ring)
{
    pthread_mutex_destroy(&ring->mutex);
}

void ring_buffer_stop(so_ring_buffer_t *ring)
{
    return;
    // pthread_mutex_lock(&ring->mutex);
    // pthread_mutex_unlock(&ring->mutex);
}
