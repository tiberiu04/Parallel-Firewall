// SPDX-License-Identifier: BSD-3-Clause

#include "ring_buffer.h"
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

// Error codes
#define RING_BUFFER_OK 0
#define RING_BUFFER_ERROR 1

// Buffer type
#define RING_BUFFER_TYPE char

// These functions are used for synchronization
static void ring_buffer_lock(so_ring_buffer_t *ring)
{
	pthread_mutex_lock(&ring->lock);
}

static void ring_buffer_unlock(so_ring_buffer_t *ring)
{
	pthread_mutex_unlock(&ring->lock);
}

static void ring_buffer_signal_not_empty(so_ring_buffer_t *ring)
{
	pthread_cond_signal(&ring->not_empty);
}

static void ring_buffer_signal_not_full(so_ring_buffer_t *ring)
{
	pthread_cond_signal(&ring->not_full);
}

static void ring_buffer_wait_not_empty(so_ring_buffer_t *ring)
{
	pthread_cond_wait(&ring->not_empty, &ring->lock);
}

static void ring_buffer_wait_not_full(so_ring_buffer_t *ring)
{
	pthread_cond_wait(&ring->not_full, &ring->lock);
}

// These functions are used to help for memory allocation
static void *ring_buffer_alloc(size_t size)
{
	return malloc(size);
}

static void ring_buffer_free(void *ptr)
{
	free(ptr);
}

// Zeroing out the ring buffer state
static void ring_buffer_reset_state(so_ring_buffer_t *ring)
{
	ring->len = 0;
	ring->read_pos = 0;
	ring->write_pos = 0;
}

static int ring_buffer_is_empty(so_ring_buffer_t *ring)
{
	return ring->len == 0;
}

static int ring_buffer_should_stop(so_ring_buffer_t *ring)
{
	return ring->stop;
}

// Handling split data operations (read or write)
static void ring_buffer_handle_split(so_ring_buffer_t *ring, void *buffer,
									 size_t size, size_t offset, int write)
{
	size_t first_part = ring->cap - offset;

	if (size <= first_part) {
		if (write)
			memcpy(ring->data + offset, buffer, size);
		else
			memcpy(buffer, ring->data + offset, size);
	} else {
		if (write) {
			memcpy(ring->data + offset, buffer, first_part);
			memcpy(ring->data, (char *)buffer + first_part, size - first_part);
		} else {
			memcpy(buffer, ring->data + offset, first_part);
			memcpy((char *)buffer + first_part, ring->data, size - first_part);
		}
	}
}

// Writing data to the ring buffer
static void ring_buffer_write(so_ring_buffer_t *ring, void *data, size_t size)
{
	ring_buffer_handle_split(ring, data, size, ring->write_pos, 1); // 1 for write
	ring->write_pos = (ring->write_pos + size) % ring->cap;
	ring->len += size;
}

// Reading data from the ring buffer
static void ring_buffer_read(so_ring_buffer_t *ring, void *dest, size_t size)
{
	ring_buffer_handle_split(ring, dest, size, ring->read_pos, 0); // 0 for read
	ring->read_pos = (ring->read_pos + size) % ring->cap;
	ring->len -= size;
}

int ring_buffer_init(so_ring_buffer_t *ring, size_t cap)
{
	if (!ring || cap == 0)
		return RING_BUFFER_ERROR;

	ring->data = (RING_BUFFER_TYPE *)ring_buffer_alloc(cap * sizeof(RING_BUFFER_TYPE));
	if (!ring->data)
		return RING_BUFFER_ERROR;

	ring->cap = cap;
	ring_buffer_reset_state(ring);
	ring->stop = 0;

	pthread_mutex_init(&ring->lock, NULL);
	pthread_cond_init(&ring->not_empty, NULL);
	pthread_cond_init(&ring->not_full, NULL);

	return RING_BUFFER_OK;
}

static int ring_buffer_has_space(so_ring_buffer_t *ring, size_t size)
{
	// Checking if there is enough space to write the data
	return ring->len + size <= ring->cap;
}

static int ring_buffer_has_data(so_ring_buffer_t *ring, size_t size)
{
	// Checking if there is enough data to read
	return ring->len >= size;
}

ssize_t ring_buffer_enqueue(so_ring_buffer_t *ring, void *data, size_t size)
{
	if (!ring || !data || !size)
		return -1;

	ring_buffer_lock(ring);

	// Waiting until there is enough space in the ring buffer
	while (!ring_buffer_has_space(ring, size) && !ring_buffer_should_stop(ring))
		ring_buffer_wait_not_full(ring);

	// If the ring buffer is stopped and there is no space, return -1
	if (ring_buffer_should_stop(ring)) {
		ring_buffer_unlock(ring);
		return -1;
	}

	ring_buffer_write(ring, data, size);
	ring_buffer_signal_not_empty(ring);

	ring_buffer_unlock(ring);
	return size;
}

ssize_t ring_buffer_dequeue(so_ring_buffer_t *ring, void *data, size_t size)
{
	if (!ring || !data || !size)
		return -1;

	ring_buffer_lock(ring);

	// Waiting until there is enough data in the ring buffer
	while (!ring_buffer_has_data(ring, size) && !ring_buffer_should_stop(ring))
		ring_buffer_wait_not_empty(ring);

	// If the ring buffer is stopped and there is no data, return -1
	if (ring_buffer_should_stop(ring) && ring_buffer_is_empty(ring)) {
		ring_buffer_unlock(ring);
		return -1;
	}

	// If there is not enough data, I read the available data
	ring_buffer_read(ring, data, size);
	ring_buffer_signal_not_full(ring);

	ring_buffer_unlock(ring);
	return size;
}

void ring_buffer_destroy(so_ring_buffer_t *ring)
{
	if (!ring)
		return;

	ring_buffer_free(ring->data);
	pthread_mutex_destroy(&ring->lock);
	pthread_cond_destroy(&ring->not_empty);
	pthread_cond_destroy(&ring->not_full);
}

void ring_buffer_stop(so_ring_buffer_t *ring)
{
	if (!ring)
		return;

	ring_buffer_lock(ring);
	ring->stop = 1;
	pthread_cond_broadcast(&ring->not_empty);
	pthread_cond_broadcast(&ring->not_full);
	ring_buffer_unlock(ring);
}
