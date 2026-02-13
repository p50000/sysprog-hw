#include "corobus.h"

#include "libcoro.h"
#include "rlist.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <vector>

/**
 * One coroutine waiting to be woken up in a list of other
 * suspended coros.
 */
struct wakeup_entry {
	struct rlist base;
	struct coro *coro;
};

/** A queue of suspended coros waiting to be woken up. */
struct wakeup_queue {
	struct rlist coros;
};

/** Suspend the current coroutine until it is woken up. */
static void
wakeup_queue_suspend_this(struct wakeup_queue *queue)
{
	struct wakeup_entry entry;
	entry.coro = coro_this();
	rlist_add_tail_entry(&queue->coros, &entry, base);
	coro_suspend();
	rlist_del_entry(&entry, base);
}

/** Wakeup the first coroutine in the queue. */
static void
wakeup_queue_wakeup_first(struct wakeup_queue *queue)
{
	if (rlist_empty(&queue->coros))
		return;
	struct wakeup_entry *entry = rlist_first_entry(&queue->coros,
		struct wakeup_entry, base);
	coro_wakeup(entry->coro);
}

struct coro_bus_channel {
	/** Channel max capacity. */
	size_t size_limit;
	/** Coroutines waiting until the channel is not full. */
	struct wakeup_queue send_queue;
	/** Coroutines waiting until the channel is not empty. */
	struct wakeup_queue recv_queue;
	/** Message queue. */
	std::vector<unsigned> data;
};

struct coro_bus {
	struct coro_bus_channel **channels;
	int channel_count;
};

static enum coro_bus_error_code global_error = CORO_BUS_ERR_NONE;

enum coro_bus_error_code
coro_bus_errno(void)
{
	return global_error;
}

void
coro_bus_errno_set(enum coro_bus_error_code err)
{
	global_error = err;
}

struct coro_bus_channel* init_channel(size_t size_limit) {
	struct coro_bus_channel *new_channel = new struct coro_bus_channel;
	new_channel->size_limit = size_limit;
	rlist_create(&new_channel->recv_queue.coros);
	rlist_create(&new_channel->send_queue.coros);
	return new_channel;
}

void send_to_free_channel(struct coro_bus_channel* chosen_channel, unsigned data) {
	assert(chosen_channel->data.size() < chosen_channel->size_limit && "Channel cannot be full");
	chosen_channel->data.push_back(data);
	if (!rlist_empty(&chosen_channel->recv_queue.coros)) {
		wakeup_queue_wakeup_first(&chosen_channel->recv_queue);
	}
}

int check_channel_existence(struct coro_bus* bus, int channel) {
	if (channel < 0 || channel >= bus->channel_count || bus->channels[channel] == nullptr) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	return 0;
}

struct coro_bus *
coro_bus_new(void)
{
	struct coro_bus* bus = new struct coro_bus;

	bus->channels = nullptr;
	bus->channel_count = 0;

	return bus;
}

void
coro_bus_delete(struct coro_bus *bus)
{
	for (int i = 0; i < bus->channel_count; i++) {
		if (bus->channels[i] != nullptr) {
			assert(rlist_empty(&bus->channels[i]->send_queue.coros) && 
			       "Channel has suspended send coroutines");
			assert(rlist_empty(&bus->channels[i]->recv_queue.coros) && 
			       "Channel has suspended receive coroutines");
			
			delete bus->channels[i];
			bus->channels[i] = nullptr;
		}
	}
	
	delete[] bus->channels;
	
	delete bus;
}

int
coro_bus_channel_open(struct coro_bus *bus, size_t size_limit)
{
	if (bus == nullptr) {
		return -1;
	}

	for (int i = 0; i < bus->channel_count; i++) {
		if (bus->channels[i] == nullptr) {
			struct coro_bus_channel* new_channel = init_channel(size_limit);
			bus->channels[i] = new_channel;
			return i;
		}
	}
	
	coro_bus_channel** new_channels = new coro_bus_channel*[bus->channel_count + 1];
	for (int i = 0; i < bus->channel_count; i++) {
		new_channels[i] = bus->channels[i];
	}
	new_channels[bus->channel_count] = init_channel(size_limit);

	delete [] bus->channels;
	bus->channels = new_channels;

	bus->channel_count++;
	return bus->channel_count - 1;
}

void
coro_bus_channel_close(struct coro_bus *bus, int channel)
{
	if (check_channel_existence(bus, channel) == -1) {
		return;
	}
	struct coro_bus_channel* chosen_channel = bus->channels[channel]; 
	
	bus->channels[channel] = NULL;
	while (!rlist_empty(&chosen_channel->send_queue.coros)) {
		wakeup_queue_wakeup_first(&chosen_channel->send_queue);
		coro_yield();
	}
	while (!rlist_empty(&chosen_channel->recv_queue.coros)) {
		wakeup_queue_wakeup_first(&chosen_channel->recv_queue);
		coro_yield();
	}
	
	delete chosen_channel;
}

int
coro_bus_send(struct coro_bus *bus, int channel, unsigned data)
{
	if (check_channel_existence(bus, channel) == -1) {
		return -1;
	}
	struct coro_bus_channel* chosen_channel = bus->channels[channel]; 
	while (coro_bus_try_send(bus, channel, data) < 0) {
		if (coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL) {
			return -1;
		}
		wakeup_queue_suspend_this(&chosen_channel->send_queue);
	}
	
	if (chosen_channel->data.size() < chosen_channel->size_limit && !rlist_empty(&chosen_channel->send_queue.coros)) {
		wakeup_queue_wakeup_first(&chosen_channel->send_queue);
	}
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return 0;
}

int
coro_bus_try_send(struct coro_bus *bus, int channel, unsigned data)
{
	if (check_channel_existence(bus, channel) == -1) {
		return -1;
	}

	struct coro_bus_channel* chosen_channel = bus->channels[channel]; 
	if (chosen_channel->data.size() < chosen_channel->size_limit) {
		send_to_free_channel(chosen_channel, data);
		coro_bus_errno_set(CORO_BUS_ERR_NONE);
		return 0;
	} else {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
}

int
coro_bus_recv(struct coro_bus *bus, int channel, unsigned *data)
{
	if (check_channel_existence(bus, channel) == -1) {
		return -1;
	}

	struct coro_bus_channel* chosen_channel = bus->channels[channel]; 
	while (coro_bus_try_recv(bus, channel, data) < 0) {
		if (coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL) {
			return -1;
		}
		wakeup_queue_suspend_this(&chosen_channel->recv_queue);
	}

	if (!chosen_channel->data.empty() && !rlist_empty(&chosen_channel->recv_queue.coros)) {
		wakeup_queue_wakeup_first(&chosen_channel->recv_queue);
	}
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return 0;
}

int
coro_bus_try_recv(struct coro_bus *bus, int channel, unsigned *data)
{
	if (check_channel_existence(bus, channel) == -1) {
		return -1;
	}

	struct coro_bus_channel* chosen_channel = bus->channels[channel]; 
	if (chosen_channel->data.size() > 0) {
		*data = chosen_channel->data.front();
		chosen_channel->data.erase(chosen_channel->data.begin());

		if (!rlist_empty(&chosen_channel->send_queue.coros)) {
			wakeup_queue_wakeup_first(&chosen_channel->send_queue);
		}
		return 0;
	} else {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
}


#if NEED_BROADCAST

int
coro_bus_broadcast(struct coro_bus *bus, unsigned data)
{
	while (coro_bus_try_broadcast(bus, data) < 0) {
		if (coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL) {
			return -1;
		}
		for (int i = 0; i < bus->channel_count; i++) {
			if (bus->channels[i] != nullptr  
				&& bus->channels[i]->data.size() >= bus->channels[i]->size_limit) {
				wakeup_queue_suspend_this(&bus->channels[i]->send_queue);
				break;
			}
		}
	}
	for (int i = 0; i < bus->channel_count; i++) {
		if (bus->channels[i] != nullptr 
			&& bus->channels[i]->data.size() < bus->channels[i]->size_limit && !rlist_empty(&bus->channels[i]->send_queue.coros)) {
			wakeup_queue_wakeup_first(&bus->channels[i]->send_queue);
		}
	}
	return 0;
}

int
coro_bus_try_broadcast(struct coro_bus *bus, unsigned data)
{
	bool has_channels = false;
	for (int i = 0; i < bus->channel_count; i++) {
		if (bus->channels[i] != nullptr) {
			has_channels = true;
		}
	}
	if (!has_channels) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	for (int i = 0; i < bus->channel_count; i++) {
		if (bus->channels[i] != nullptr 
			&& !(bus->channels[i]->data.size() < bus->channels[i]->size_limit)) {
			coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
			return -1;
		}
	}
	for (int i = 0; i < bus->channel_count; i++) {
		if (bus->channels[i] != nullptr) { 
			send_to_free_channel(bus->channels[i], data);
		}
	}
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return 0;
}

#endif

#if NEED_BATCH

int
coro_bus_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count)
{
	if (check_channel_existence(bus, channel) == -1) {
		return -1;
	}

	struct coro_bus_channel* chosen_channel = bus->channels[channel]; 
	int sent = coro_bus_try_send_v(bus, channel, data, count);
	while (sent < 0) {
		if (coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL) {
			return -1;
		}
		wakeup_queue_suspend_this(&chosen_channel->send_queue);
		sent = coro_bus_try_send_v(bus, channel, data, count);
	}
	
	if (chosen_channel->data.size() < chosen_channel->size_limit && !rlist_empty(&chosen_channel->send_queue.coros)) {
		wakeup_queue_wakeup_first(&chosen_channel->send_queue);
	}
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return sent;
}

int
coro_bus_try_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count)
{
	if (check_channel_existence(bus, channel) == -1) {
		return -1;
	}

	struct coro_bus_channel* chosen_channel = bus->channels[channel]; 
	if (chosen_channel->data.size() < chosen_channel->size_limit) {
		unsigned sent = 0;
		while (chosen_channel->data.size() < chosen_channel->size_limit && sent < count) {
			chosen_channel->data.push_back(data[sent]);
			sent++;
		}
		if (!rlist_empty(&chosen_channel->recv_queue.coros)) {
			wakeup_queue_wakeup_first(&chosen_channel->recv_queue);
		}
		return sent;
	} else {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
}

int
coro_bus_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity)
{
	if (check_channel_existence(bus, channel) == -1) {
		return -1;
	}

	struct coro_bus_channel* chosen_channel = bus->channels[channel]; 
	int received = coro_bus_try_recv_v(bus, channel, data, capacity);
	while (received < 0) {
		if (coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL) {
			return -1;
		}
		wakeup_queue_suspend_this(&chosen_channel->recv_queue);
		received = coro_bus_try_recv_v(bus, channel, data, capacity);
	}

	if (!chosen_channel->data.empty() && !rlist_empty(&chosen_channel->recv_queue.coros)) {
		wakeup_queue_wakeup_first(&chosen_channel->recv_queue);
	}
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return received;
}

int
coro_bus_try_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity)
{
	if (check_channel_existence(bus, channel) == -1) {
		return -1;
	}

	struct coro_bus_channel* chosen_channel = bus->channels[channel]; 
	if (chosen_channel->data.size() > 0) {
		unsigned received = 0;
		while (chosen_channel->data.size() > 0 && received < capacity) {
			data[received] = chosen_channel->data.front();
			chosen_channel->data.erase(chosen_channel->data.begin());
			received++;
		}

		if (!rlist_empty(&chosen_channel->send_queue.coros)) {
			wakeup_queue_wakeup_first(&chosen_channel->send_queue);
		}
		return received;
	} else {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
}

#endif
