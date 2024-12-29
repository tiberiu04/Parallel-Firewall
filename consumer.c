// SPDX-License-Identifier: BSD-3-Clause
#include "consumer.h"
#include <stdlib.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include "ring_buffer.h"
#include "packet.h"
#include "utils.h"

pthread_mutex_t seq_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t seq_cond = PTHREAD_COND_INITIALIZER;
unsigned long sequence_number;    // For packets upon deque
unsigned long next_expected_seq; // The next sequence expected to be written

static int open_output_file(const char *out_filename)
{
	int fd = open(out_filename, O_RDWR | O_CREAT | O_APPEND, 0666);

	if (fd < 0)
		perror("Failed to open output file");

	return fd;
}

// Processing and preparing output string for a packet
static int process_and_format_packet(struct so_packet_t *pkt, char *out_buf,
									 size_t buf_size)
{
	int action = process_packet(pkt);
	unsigned long hash = packet_hash(pkt);
	unsigned long timestamp = pkt->hdr.timestamp;

	return snprintf(out_buf, buf_size, "%s %016lx %lu\n",
					RES_TO_STR(action), hash, timestamp);
}

// Writing output in the correct order
static void write_output_in_order(int fd, const char *out_buf, int len,
								  unsigned long my_seq)
{
	pthread_mutex_lock(&seq_mutex);

	while (my_seq != next_expected_seq)
		pthread_cond_wait(&seq_cond, &seq_mutex);

	// Writing to file
	write(fd, out_buf, len);

	// Updating sequence and notifying other threads
	next_expected_seq++;
	pthread_cond_broadcast(&seq_cond);

	pthread_mutex_unlock(&seq_mutex);
}

void consumer_thread(so_consumer_ctx_t *ctx)
{
	ssize_t sz;
	int out_fd = open_output_file(ctx->out_filename);

	if (out_fd < 0)
		return;

	char buffer[PKT_SZ];
	char out_buf[256];
	unsigned long my_seq;

	while (1) {
		// Dequeuing a packet
		pthread_mutex_lock(&seq_mutex);
		sz = ring_buffer_dequeue(ctx->producer_rb, buffer, PKT_SZ);
		if (sz <= 0 && ctx->producer_rb->stop) {
			pthread_mutex_unlock(&seq_mutex);
			break;
		}
		my_seq = sequence_number++;
		pthread_mutex_unlock(&seq_mutex);

		// Processing the packet and format output
		struct so_packet_t *pkt = (struct so_packet_t *)buffer;
		int len = process_and_format_packet(pkt, out_buf, sizeof(out_buf));

		// Writing the output in the correct order
		write_output_in_order(out_fd, out_buf, len, my_seq);
	}

	close(out_fd);
}

static void initialize_consumer_context(so_consumer_ctx_t *ctx,
										struct so_ring_buffer_t *rb,
										const char *out_filename)
{
	ctx->producer_rb = rb;
	ctx->out_filename = out_filename;
}

int create_consumers(pthread_t *tids, int num_consumers,
					 struct so_ring_buffer_t *rb, const char *out_filename)
{
	so_consumer_ctx_t *ctx =
	(so_consumer_ctx_t *)malloc(num_consumers * sizeof(so_consumer_ctx_t));

	if (!ctx)
		return -1;

	for (int i = 0; i < num_consumers; i++) {
		initialize_consumer_context(&ctx[i], rb, out_filename);
		pthread_create(&tids[i], NULL, (void *)consumer_thread, &ctx[i]);
	}

	return num_consumers;
}
