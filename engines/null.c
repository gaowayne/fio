/*
 * null engine
 *
 * IO engine that doesn't do any real IO transfers, it just pretends to.
 * The main purpose is to test fio itself.
 *
 * It also can act as external C++ engine - compiled with:
 *
 * g++ -O2 -g -shared -rdynamic -fPIC -o cpp_null null.c \
 *	-include ../config-host.h -DFIO_EXTERNAL_ENGINE
 *
 * to test it execute:
 *
 * LD_LIBRARY_PATH=./engines ./fio examples/cpp_null.fio
 *
 */
#include <stdlib.h>
#include <assert.h>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <iostream>
#include <vector>
#include <bitset>
#include <queue>
#include <memory>
#include <unordered_map>

#include "../fio.h"

#define FIO_EXTERNAL_ENGINE 1
#define WSZ_SECTOR_SHIFT		  12
#define WSZ_ZONE_SHIFT            26
#define WSZ_RANDOM_DEVICE_SHIFT   27
#define WSZ_SEQUENCE_DEVICE_SHIFT 32
#define WSZ_SECTOR_SIZE		      (1 << WSZ_SECTOR_SHIFT)
#define WSZ_ZONE_SIZE	          (1 << WSZ_ZONE_SHIFT)
#define WSZ_ZONE_LENGTH           (1 << (WSZ_ZONE_SHIFT - WSZ_SECTOR_SHIFT))
#define WSZ_RANDOM_LENGTH         (1 << (WSZ_RANDOM_DEVICE_SHIFT - WSZ_ZONE_SHIFT))
#define WSZ_SEQUENCE_LENGTH       (1 << (WSZ_SEQUENCE_DEVICE_SHIFT - WSZ_ZONE_SHIFT))

using namespace boost::asio;

struct dm_zone {
    int offset;
    int point;
    void* buffer;
    dm_zone() {
        buffer = malloc(WSZ_ZONE_SIZE);
        offset = 0;
        point = -1;
    }
};
struct rnd_device_table {
    std::vector<dm_zone> zones;
    std::vector<std::shared_ptr<std::bitset<WSZ_ZONE_LENGTH>>> bitmaps;
    int offset;
    rnd_device_table() {
        offset = 0;
        for (int i = 0; i < WSZ_RANDOM_LENGTH; ++i) {
            zones.emplace_back(dm_zone());
        }
    }
};

struct seq_device_table {
    std::vector<dm_zone> zones;
    std::queue<int> free_zones_queue;
    seq_device_table() {
        for (int i = 0; i < WSZ_SEQUENCE_LENGTH; ++i) {
            zones.emplace_back(dm_zone());
            free_zones_queue.push(i);
        }
    }
};

struct null_data {
	struct io_u **io_us;
	int queued;
	int events;
    std::shared_ptr<std::unordered_map<int, int>> zone_map;
    std::shared_ptr<seq_device_table> seqDeviceTable;
    std::shared_ptr<rnd_device_table> rndDeviceTable;
};


static struct io_u *null_event(struct null_data *nd, int event)
{
	return nd->io_us[event];
}

static int null_getevents(struct null_data *nd, unsigned int min_events,
			  unsigned int fio_unused max,
			  const struct timespec fio_unused *t)
{
	int ret = 0;

	if (min_events) {
		nd->events = nd->queued;// give all queued into events wayne

		ret = nd->events;
		nd->events = 0;

		nd->queued = 0;//removed queued at once wayne
	}

	return ret;
}


static int write2seq(struct null_data *nd, struct io_u *io_u)
{
    int io_zone_no = io_u->offset / WSZ_ZONE_SIZE;
    if (nd->zone_map->find(io_zone_no) == nd->zone_map->end()) {
        if (!nd->seqDeviceTable->free_zones_queue.empty()) {
            int zone_no = nd->seqDeviceTable->free_zones_queue.front();
            nd->seqDeviceTable->free_zones_queue.pop();
            nd->zone_map->insert({io_zone_no, zone_no});
        }
        else
            return -1;
    }
    int zone_no = nd->zone_map->at(io_zone_no);
    memcpy(nd->seqDeviceTable->zones[zone_no].buffer + io_u->offset % WSZ_ZONE_SIZE,
           io_u->buf, io_u->buflen);
    nd->seqDeviceTable->zones[zone_no].offset += io_u->buflen / WSZ_SECTOR_SIZE;
    return 1;
}

static int reclaim(struct null_data *nd)
{
    for (int i = 0; i < WSZ_RANDOM_LENGTH; ++i) {
        if (nd->rndDeviceTable->zones[i].point == -1)
            return -1;
        if (nd->seqDeviceTable->free_zones_queue.empty())
            return -1;
        int seq_no = nd->zone_map->at(nd->rndDeviceTable->zones[i].point);
        int target_zone_no = nd->seqDeviceTable->free_zones_queue.front();
        nd->seqDeviceTable->free_zones_queue.pop();
        for (int j = 0; j < nd->seqDeviceTable->zones[seq_no].offset; ++j) {
            int offset = j * WSZ_SECTOR_SIZE;
            if (nd->rndDeviceTable->bitmaps[i]->test(j))
                memcpy(nd->seqDeviceTable->zones[target_zone_no].buffer + offset,
                       nd->rndDeviceTable->zones[i].buffer + offset, WSZ_SECTOR_SIZE);
            else
                memcpy(nd->seqDeviceTable->zones[target_zone_no].buffer + offset,
                       nd->seqDeviceTable->zones[seq_no].buffer + offset, WSZ_SECTOR_SIZE);
        }
        nd->seqDeviceTable->zones[seq_no] = dm_zone();
        nd->zone_map->at(nd->rndDeviceTable->zones[i].point) = target_zone_no;
        nd->seqDeviceTable->free_zones_queue.push(seq_no);
        nd->rndDeviceTable->zones[i] = dm_zone();
    }
    nd->rndDeviceTable->offset = 0;
    nd->rndDeviceTable->bitmaps.clear();
    return 1;
}

static int write2rnd(struct null_data *nd, struct io_u *io_u)
{
    int io_zone_no = io_u->offset / WSZ_ZONE_SIZE;
    int seq_zone_no = nd->zone_map->at(io_zone_no);
    int iq_offset = io_u->offset % WSZ_ZONE_SIZE;
    int iq_sector_no = iq_offset / WSZ_SECTOR_SIZE;
    if (nd->seqDeviceTable->zones[seq_zone_no].point >= 0) {
        int rnd_zone_no = nd->zone_map->at(nd->seqDeviceTable->zones[seq_zone_no].point);
        memcpy(nd->rndDeviceTable->zones[rnd_zone_no].buffer + iq_offset,
               io_u->buf, io_u->buflen);
        nd->rndDeviceTable->bitmaps[rnd_zone_no]->set(iq_sector_no, true);
        return 1;
    }
    if (nd->rndDeviceTable->offset == WSZ_RANDOM_LENGTH)
        reclaim(nd);
    int rnd_zone_no = nd->rndDeviceTable->offset;
    std::shared_ptr<std::bitset<WSZ_ZONE_LENGTH>> b(new std::bitset<WSZ_ZONE_LENGTH>());
    nd->rndDeviceTable->bitmaps.emplace_back(b);
    memcpy(nd->rndDeviceTable->zones[rnd_zone_no].buffer + iq_offset,
           io_u->buf, io_u->buflen);
    nd->rndDeviceTable->bitmaps[rnd_zone_no]->set(iq_sector_no);
    nd->seqDeviceTable->zones[seq_zone_no].point = rnd_zone_no;
    nd->rndDeviceTable->zones[rnd_zone_no].point = io_zone_no;
    nd->rndDeviceTable->offset = rnd_zone_no + 1;
    return 1;
}

static bool check_offset (struct null_data *nd, struct io_u *io_u)
{
    int zone_no = nd->zone_map->at(io_u->offset / WSZ_ZONE_SIZE);
    int iq_offset = (io_u->offset % WSZ_ZONE_SIZE)/ WSZ_SECTOR_SIZE;
    if (iq_offset == nd->seqDeviceTable->zones[zone_no].offset)
        return true;
    return false;
}

static int read(struct null_data *nd, struct io_u *io_u) {
    int zone_no = nd->zone_map->at(io_u->offset / WSZ_ZONE_SIZE);
    int iq_offset = io_u->offset % WSZ_ZONE_SIZE;
    int iq_sector_no = iq_offset / WSZ_SECTOR_SIZE;
    if (nd->seqDeviceTable->zones[zone_no].point >= 0) {
        int rdt_zone_no = nd->seqDeviceTable->zones[zone_no].point;
        if (nd->rndDeviceTable->bitmaps[rdt_zone_no]->test(iq_sector_no)) {
            memcpy(io_u->buf, nd->rndDeviceTable->zones[rdt_zone_no].buffer + iq_offset * WSZ_SECTOR_SIZE,
                   io_u->buflen * WSZ_SECTOR_SIZE);
            return 1;
        }
    }
    memcpy(io_u->buf, nd->seqDeviceTable->zones[zone_no].buffer + iq_offset * WSZ_SECTOR_SIZE,
           io_u->buflen * WSZ_SECTOR_SIZE);
    return 1;
}
static enum fio_q_status null_queue(struct thread_data *td,
				    struct null_data *nd, struct io_u *io_u)
{
	fio_ro_check(td, io_u);

    if (nd->events)
        return FIO_Q_BUSY;

    int flag = 0;
	if (td->io_ops->flags & FIO_SYNCIO) {
        if (io_u->ddir == DDIR_READ)
            flag = read(nd, io_u);
        else if (io_u->ddir == DDIR_WRITE) {
            if (nd->zone_map->find(io_u->offset / WSZ_ZONE_SIZE) == nd->zone_map->end())
                flag = write2seq(nd, io_u);
            else if (check_offset(nd, io_u))
                flag = write2seq(nd, io_u);
            else
                flag = write2rnd(nd, io_u);
        }
        return FIO_Q_COMPLETED;
    }

	nd->io_us[nd->queued++] = io_u;
	return FIO_Q_QUEUED;
}

static int null_open(struct null_data fio_unused *nd,
		     struct fio_file fio_unused *f)
{
	return 0;
}

static void null_cleanup(struct null_data *nd)
{
	if (nd) {
		free(nd->io_us);
		free(nd);
	}
}

static struct null_data *null_init(struct thread_data *td)
{
	struct null_data *nd = (struct null_data *) malloc(sizeof(*nd));

	memset(nd, 0, sizeof(*nd));

    nd->seqDeviceTable.reset(new seq_device_table());
    nd->rndDeviceTable.reset(new rnd_device_table());
    nd->zone_map.reset(new std::unordered_map<int, int>());

	if (td->o.iodepth != 1) {
		nd->io_us = (struct io_u **) malloc(td->o.iodepth * sizeof(struct io_u *));
		memset(nd->io_us, 0, td->o.iodepth * sizeof(struct io_u *));
	} else
		td->io_ops->flags |= FIO_SYNCIO;

	return nd;
}

#define __cplusplus 1

#ifndef __cplusplus

static struct io_u *fio_null_event(struct thread_data *td, int event)
{
	return null_event(td->io_ops_data, event);
}

static int fio_null_getevents(struct thread_data *td, unsigned int min_events,
			      unsigned int max, const struct timespec *t)
{
	struct null_data *nd = td->io_ops_data;
	return null_getevents(nd, min_events, max, t);
}

static int fio_null_commit(struct thread_data *td)
{
	return null_commit(td, td->io_ops_data);
}

static enum fio_q_status fio_null_queue(struct thread_data *td,
					struct io_u *io_u)
{
	return null_queue(td, td->io_ops_data, io_u);
}

static int fio_null_open(struct thread_data *td, struct fio_file *f)
{
	return null_open(td->io_ops_data, f);
}

static void fio_null_cleanup(struct thread_data *td)
{
	null_cleanup(td->io_ops_data);
}

static int fio_null_init(struct thread_data *td)
{
	td->io_ops_data = null_init(td);
	assert(td->io_ops_data);
	return 0;
}

static struct ioengine_ops ioengine = {
	.name		= "null",
	.version	= FIO_IOOPS_VERSION,
	.queue		= fio_null_queue,
	.commit		= fio_null_commit,
	.getevents	= fio_null_getevents,
	.event		= fio_null_event,
	.init		= fio_null_init,
	.cleanup	= fio_null_cleanup,
	.open_file	= fio_null_open,
	.flags		= FIO_DISKLESSIO | FIO_FAKEIO,
};

static void fio_init fio_null_register(void)
{
	register_ioengine(&ioengine);
}

static void fio_exit fio_null_unregister(void)
{
	unregister_ioengine(&ioengine);
}

#else

#ifdef FIO_EXTERNAL_ENGINE

io_service service;

void func(int i) {
	std::cout << "func called, i= " << i << std::endl;
}

void asio_queue_io(struct null_data* nd, struct io_u* io_u) {
	if (nd && io_u)
	{
		//nd->io_us[nd->queued++] = io_u;
		std::cout << "asio_queue_io, queued= " << nd->queued << std::endl;
	}
	else
	{
		std::cout << "wayne nd=0 io_u=0 called, i= " << std::endl;
	}
}

void worker_thread() {
	service.run();
	// wait for all threads to be created
	boost::this_thread::sleep(boost::posix_time::millisec(10000));
}

struct NullData {
	NullData(struct thread_data *td)
	{
		impl_ = null_init(td);
		assert(impl_);
	}

	~NullData()
	{
		null_cleanup(impl_);
	}

	static NullData *get(struct thread_data *td)
	{
		return reinterpret_cast<NullData *>(td->io_ops_data);
	}

	io_u *fio_null_event(struct thread_data *, int event)
	{
		return null_event(impl_, event);
	}

	int fio_null_getevents(struct thread_data *, unsigned int min_events,
			       unsigned int max, const struct timespec *t)
	{
		return null_getevents(impl_, min_events, max, t);
	}

	int fio_null_commit(struct thread_data *td)
	{
		return 0;// null_commit(td, impl_);
	}

	fio_q_status fio_null_queue(struct thread_data *td, struct io_u *io_u)
	{
		//service.post(boost::bind(func, 15));//reinterpret_cast<int>(io_u)
		//return null_queue(td, impl_, io_u);
		struct null_data* nd = (struct null_data*)impl_;
        fio_ro_check(td, io_u);

        if (nd->events)
            return FIO_Q_BUSY;

        int flag = 0;
        if (td->io_ops->flags & FIO_SYNCIO) {
            if (io_u->ddir == DDIR_READ)
                flag = read(nd, io_u);
            else if (io_u->ddir == DDIR_WRITE) {
                if (nd->zone_map->find(io_u->offset / WSZ_ZONE_SIZE) == nd->zone_map->end())
                    flag = write2seq(nd, io_u);
                else if (check_offset(nd, io_u))
                    flag = write2seq(nd, io_u);
                else
                    flag = write2rnd(nd, io_u);
            }
            return FIO_Q_COMPLETED;
        }

        nd->io_us[nd->queued++] = io_u;
        return FIO_Q_QUEUED;
//		fio_ro_check(td, io_u);
//
//		if (td->io_ops->flags & FIO_SYNCIO)
//			return FIO_Q_COMPLETED;
//		if (nd->events)// if > qd then it is busy let us use qd=128
//			return FIO_Q_BUSY;
//
//		nd->io_us[nd->queued++] = io_u;
//		service.post(boost::bind(asio_queue_io, nd, io_u));
//		return FIO_Q_QUEUED;
	}

	int fio_null_open(struct thread_data *, struct fio_file *f)
	{
		threads.create_thread(worker_thread);

		service.post(boost::bind(asio_queue_io, (null_data*)0, (io_u*)0));
		return null_open(impl_, f);
	}

public:

	boost::thread_group threads;

private:
	struct null_data *impl_;

};

extern "C" {

static struct io_u *fio_null_event(struct thread_data *td, int event)
{
	return NullData::get(td)->fio_null_event(td, event);
}

static int fio_null_getevents(struct thread_data *td, unsigned int min_events,
			      unsigned int max, const struct timespec *t)
{
	return NullData::get(td)->fio_null_getevents(td, min_events, max, t);
}

static int fio_null_commit(struct thread_data *td)
{
	//return NullData::get(td)->fio_null_commit(td);
	return 0;
}

static fio_q_status fio_null_queue(struct thread_data *td, struct io_u *io_u)
{
	return NullData::get(td)->fio_null_queue(td, io_u);
}

static int fio_null_open(struct thread_data *td, struct fio_file *f)
{
	return NullData::get(td)->fio_null_open(td, f);
}

static int fio_null_init(struct thread_data *td)
{
//	                int p = 0;
//                printf("Enter init in null: ");
//		sleep(10);
//                scanf("%d", &p);
	td->io_ops_data = new NullData(td);
	return 0;
}

static void fio_null_cleanup(struct thread_data *td)
{
	delete NullData::get(td);
}

static struct ioengine_ops ioengine;
void get_ioengine(struct ioengine_ops **ioengine_ptr)
{
	*ioengine_ptr = &ioengine;

	ioengine.name           = "cpp_null";
	ioengine.version        = FIO_IOOPS_VERSION;
	ioengine.queue          = fio_null_queue;
//	ioengine.commit         = fio_null_commit;
	ioengine.getevents      = fio_null_getevents;
	ioengine.event          = fio_null_event;
	ioengine.init           = fio_null_init;
	ioengine.cleanup        = fio_null_cleanup;
	ioengine.open_file      = fio_null_open;
	ioengine.flags          = FIO_DISKLESSIO | FIO_FAKEIO | FIO_ASYNCIO_SYNC_TRIM | FIO_SYNCIO;
}
}
#endif /* FIO_EXTERNAL_ENGINE */

#endif /* __cplusplus */
