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
    pthread_mutex_init(&ring->cons_cond, NULL);
    pthread_cond_init(&ring->prod_cond, NULL);

    return 0;
}

ssize_t ring_buffer_enqueue(so_ring_buffer_t *ring, void *data, size_t size)
{
    if (ring == NULL || data == NULL || size == 0)
        return -1;

    pthread_mutex_lock(&ring->mutex);

    while (ring->len + size > ring->cap)
        pthread_cond_wait(&ring->prod_cond, &ring->mutex);

    memcpy(ring->data + ring->write_pos, data, size);

    ring->write_pos = (ring->write_pos + size) % ring->cap;
    ring->len += size;

    // Notifică consumatorii că sunt date disponibile
    pthread_cond_signal(&ring->cons_cond);
    pthread_mutex_unlock(&ring->mutex);

    return size;
}

ssize_t ring_buffer_dequeue(so_ring_buffer_t *ring, void *data, size_t size)
{
    if (ring == NULL || data == NULL || size == 0)
        return -1;

    pthread_mutex_lock(&ring->mutex);

    // Așteaptă până când există suficiente date pentru a citi întregul pachet
    while (ring->len < size)
        pthread_cond_wait(&ring->cons_cond, &ring->mutex);

    // Copiază întregul pachet din poziția curentă de citire
    memcpy(data, ring->data + ring->read_pos, size);

    // Actualizează poziția de citire și lungimea bufferului
    ring->read_pos = (ring->read_pos + size) % ring->cap;
    ring->len -= size;

    // Notifică producătorii că este spațiu disponibil
    pthread_cond_signal(&ring->prod_cond);
    pthread_mutex_unlock(&ring->mutex);

    return size;
}

void ring_buffer_destroy(so_ring_buffer_t *ring)
{
    if (ring == NULL)
        return;

    free(ring->data);
    pthread_mutex_destroy(&ring->mutex);
    pthread_cond_destroy(&ring->prod_cond);
    pthread_cond_destroy(&ring->cons_cond);
}

void ring_buffer_stop(so_ring_buffer_t *ring)
{
    if (ring == NULL)
        return;

    pthread_mutex_lock(&ring->mutex);
    pthread_cond_broadcast(&ring->cons_cond);
    pthread_cond_broadcast(&ring->prod_cond);
    pthread_mutex_unlock(&ring->mutex);
}
