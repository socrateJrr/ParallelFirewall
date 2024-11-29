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
    ring->stop = 0;

    pthread_mutex_init(&ring->mutex, NULL);
    pthread_cond_init(&ring->cond_cons, NULL);
    pthread_cond_init(&ring->cond_prod, NULL);

    return 0;
}

ssize_t ring_buffer_enqueue(so_ring_buffer_t *ring, void *data, size_t size)
{
    pthread_mutex_lock(&ring->mutex);

    while (ring->len + size > ring->cap)
        pthread_cond_wait(&ring->cond_prod, &ring->mutex);

    memcpy(ring->data + ring->write_pos, data, size);

    ring->write_pos = (ring->write_pos + size) % ring->cap;
    ring->len += size;

    pthread_cond_signal(&ring->cond_cons);
    pthread_mutex_unlock(&ring->mutex);

    return size;
}

ssize_t ring_buffer_dequeue(so_ring_buffer_t *ring, void *data, size_t size)
{
    pthread_mutex_lock(&ring->mutex);

    while (ring->len < size && ring->stop == 0)
        pthread_cond_wait(&ring->cond_cons, &ring->mutex);

    if (ring->len < size && ring->stop)
    {
        pthread_mutex_unlock(&ring->mutex);
        return -1;
    }

    memcpy(data, ring->data + ring->read_pos, size);

    ring->read_pos = (ring->read_pos + size) % ring->cap;
    ring->len -= size;

    pthread_cond_signal(&ring->cond_prod);
    pthread_mutex_unlock(&ring->mutex);

    return size;
}

void ring_buffer_destroy(so_ring_buffer_t *ring)
{
    pthread_mutex_destroy(&ring->mutex);
    pthread_cond_destroy(&ring->cond_prod);
    pthread_cond_destroy(&ring->cond_cons);
}

void ring_buffer_stop(so_ring_buffer_t *ring)
{
    ring->stop = 1;
    pthread_mutex_lock(&ring->mutex);
    // pthread_cond_broadcast(&ring->cond_prod);
    pthread_cond_broadcast(&ring->cond_cons);
    pthread_mutex_unlock(&ring->mutex);
}
