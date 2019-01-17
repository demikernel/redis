/* A simple event-driven programming library. Originally I wrote this code
 * for the Jim's event-loop (Jim is a Tcl interpreter) but later translated
 * it in form of a library for easy reuse.
 *
 * Copyright (c) 2006-2010, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdio.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <poll.h>
#include <string.h>
#include <time.h>
#include <errno.h>

//#include "ae.h"
#include "server.h"
#include "zmalloc.h"
#include "config.h"

/* Include the best multiplexing layer supported by this system.
 * The following should be ordered by performances, descending. */
#ifdef HAVE_EVPORT
#include "ae_evport.c"
#else
    #ifdef HAVE_EPOLL
    #include "ae_epoll.c"
    #else
        #ifdef HAVE_KQUEUE
        #include "ae_kqueue.c"
        #else
        #include "ae_select.c"
        #endif
    #endif
#endif


int add_queue_status_item(aeEventLoop *eventLoop, int qd, int status) {
    int qd_status_index = eventLoop->qd_status_array_index;
    struct qd_status *new_status = &(eventLoop->qd_status_array[qd_status_index]);
    eventLoop->qd_status_array_index++;
    // initialize queue status
    new_status->qd = qd;
    (new_status->status_token_arr)[0] = status;
    (new_status->status_token_arr)[1] = -1;
    HASH_ADD_INT(eventLoop->qd_status_map, qd, new_status);
    return qd_status_index;
}

struct qd_status* find_queue_status_item(aeEventLoop* eventLoop, int qd) {
    struct qd_status *qd_status_ptr;
    HASH_FIND_INT(eventLoop->qd_status_map, &qd, qd_status_ptr);
    return qd_status_ptr;
}

int del_queue_status_item(aeEventLoop *eventLoop, int qd){
    struct qd_status *qd_status_ptr = find_queue_status_item(eventLoop, qd);
    if(qd_status_ptr == NULL){
        return -1;
    }
    HASH_DEL(eventLoop->qd_status_map, qd_status_ptr);
}

zeus_sgarray* use_sgarray(aeEventLoop *eventLoop){
    zeus_sgarray *sga_ptr;
    if(eventLoop->sga_idx < (eventLoop->setsize*(_SGA_ALLOC_FACTOR))){
        sga_ptr = &(eventLoop->sgarray_list[eventLoop->sga_idx]);
        eventLoop->sga_idx++;
    }else{
        fprintf(stderr, "ERROR no avaiable sga\n");
        // error here
        return NULL;
    }
    //memset(sga_ptr, 0, sizeof(zeus_sgarray));
    sga_ptr->num_bufs = 0;
    //fprintf(stderr, "return sga_ptr:%p num_bufs:%d\n", sga_ptr, sga_ptr->num_bufs);
    return sga_ptr;
}

int return_sgarray(aeEventLoop *eventLoop, zeus_sgarray* sga){
    return -1;
}

aeEventLoop *aeCreateEventLoop(int setsize) {
    aeEventLoop *eventLoop;
    int i;

    if ((eventLoop = zmalloc(sizeof(*eventLoop))) == NULL) goto err;
    eventLoop->events = zmalloc(sizeof(aeFileEvent)*setsize);
    eventLoop->fired = zmalloc(sizeof(aeFiredEvent)*setsize);
    if (eventLoop->events == NULL || eventLoop->fired == NULL) goto err;
    eventLoop->setsize = setsize;
    eventLoop->lastTime = time(NULL);
    eventLoop->timeEventHead = NULL;
    eventLoop->timeEventNextId = 0;
    eventLoop->stop = 0;
    eventLoop->maxfd = -1;
    eventLoop->beforesleep = NULL;
    eventLoop->aftersleep = NULL;
    /* _JL_ init qd status map */
    // NOTE events always use fd as index
    eventLoop->qd_status_map = NULL;  /* required init for hash data structure */
    eventLoop->qd_status_array = zmalloc(sizeof(struct qd_status)*setsize);
    eventLoop->wait_qtokens = zmalloc(sizeof(zeus_qtoken)*setsize);
    eventLoop->sgarray_list = zmalloc(sizeof(zeus_sgarray)*(setsize*(_SGA_ALLOC_FACTOR)));
    eventLoop->qd_status_array_index = 0;
    eventLoop->sga_idx = 0;
    for(i = 0; i < setsize; i++){
        (eventLoop->qd_status_array[i].status_token_arr)[0] = LIBOS_Q_STATUS_NONE;
        (eventLoop->qd_status_array[i].status_token_arr)[1] = -1;
        (eventLoop->qd_status_array[i].status_token_arr)[2] = 0;
    }
    /**************************/

    if (aeApiCreate(eventLoop) == -1) goto err;
    /* Events with mask == AE_NONE are not set. So let's initialize the
     * vector with it. */
    for (i = 0; i < setsize; i++)
        eventLoop->events[i].mask = AE_NONE;
    return eventLoop;

err:
    if (eventLoop) {
        zfree(eventLoop->events);
        zfree(eventLoop->fired);
        zfree(eventLoop->qd_status_array);
        zfree(eventLoop->wait_qtokens);
        zfree(eventLoop->sgarray_list);
        zfree(eventLoop);
    }
    return NULL;
}

/* Return the current set size. */
int aeGetSetSize(aeEventLoop *eventLoop) {
    return eventLoop->setsize;
}

/* Resize the maximum set size of the event loop.
 * If the requested set size is smaller than the current set size, but
 * there is already a file descriptor in use that is >= the requested
 * set size minus one, AE_ERR is returned and the operation is not
 * performed at all.
 *
 * Otherwise AE_OK is returned and the operation is successful. */
int aeResizeSetSize(aeEventLoop *eventLoop, int setsize) {
    int i;

    if (setsize == eventLoop->setsize) return AE_OK;
    if (eventLoop->maxfd >= setsize) return AE_ERR;
    if (aeApiResize(eventLoop,setsize) == -1) return AE_ERR;

    eventLoop->events = zrealloc(eventLoop->events,sizeof(aeFileEvent)*setsize);
    eventLoop->fired = zrealloc(eventLoop->fired,sizeof(aeFiredEvent)*setsize);
    eventLoop->setsize = setsize;

    /* _JL_ re-init in re-size */
    eventLoop->qd_status_array = zrealloc(eventLoop->qd_status_array,sizeof(struct qd_status)*setsize);
    eventLoop->wait_qtokens = zrealloc(eventLoop->wait_qtokens, sizeof(zeus_qtoken)*setsize);
    eventLoop->sgarray_list = zrealloc(eventLoop->sgarray_list, sizeof(zeus_sgarray)*(setsize*(_SGA_ALLOC_FACTOR)));

    /* Make sure that if we created new slots, they are initialized with
     * an AE_NONE mask. */
    for (i = eventLoop->maxfd+1; i < setsize; i++) {
        eventLoop->events[i].mask = AE_NONE;
        (eventLoop->qd_status_array[i].status_token_arr)[0] = LIBOS_Q_STATUS_NONE;
        (eventLoop->qd_status_array[i].status_token_arr)[1] = -1;
        (eventLoop->qd_status_array[i].status_token_arr)[2] = 0;
    }
    return AE_OK;
}

void aeDeleteEventLoop(aeEventLoop *eventLoop) {
    aeApiFree(eventLoop);
    zfree(eventLoop->events);
    zfree(eventLoop->fired);
    /* _JL_ */
    zfree(eventLoop->qd_status_array);
    zfree(eventLoop->wait_qtokens);
    zfree(eventLoop->sgarray_list);
    //////////
    zfree(eventLoop);
}

void aeStop(aeEventLoop *eventLoop) {
    eventLoop->stop = 1;
}

int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask,
        aeFileProc *proc, void *clientData)
{
    /**
     * _JL_:
     * we would not create event here,
     * the return of wait any essentially
     *
     **/
#if 0
    int qd = fd;
    int cur_fd = zeus_qd2fd(qd);
    if(cur_fd < 0){
        // assume invalid fd here
        fprintf(stderr, "cannot find the qd in libos qd:%d\n", qd);
        fd = qd;
    }else{
        fd = cur_fd;
    }

    if (fd >= eventLoop->setsize) {
        errno = ERANGE;
        return AE_ERR;
    }
    aeFileEvent *fe = &eventLoop->events[fd];

    if (aeApiAddEvent(eventLoop, fd, mask) == -1)
        return AE_ERR;
    fe->mask |= mask;
    if (mask & AE_READABLE) fe->rfileProc = proc;
    if (mask & AE_WRITABLE) fe->wfileProc = proc;
    fe->clientData = clientData;
    if (fd > eventLoop->maxfd)
        eventLoop->maxfd = fd;
#endif
    return AE_OK;
}

void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask)
{
#if 0
    // printf("ae.c/aeDeleteEvent @@@@@@fd:%d\n", fd);
    if (fd >= eventLoop->setsize) return;
    aeFileEvent *fe = &eventLoop->events[fd];
    if (fe->mask == AE_NONE) return;

    /* We want to always remove AE_BARRIER if set when AE_WRITABLE
     * is removed. */
    if (mask & AE_WRITABLE) mask |= AE_BARRIER;

    aeApiDelEvent(eventLoop, fd, mask);
    fe->mask = fe->mask & (~mask);
    if (fd == eventLoop->maxfd && fe->mask == AE_NONE) {
        /* Update the max fd */
        int j;

        for (j = eventLoop->maxfd-1; j >= 0; j--)
            if (eventLoop->events[j].mask != AE_NONE) break;
        eventLoop->maxfd = j;
    }
#endif
}

int aeGetFileEvents(aeEventLoop *eventLoop, int fd) {
    if (fd >= eventLoop->setsize) return 0;
    aeFileEvent *fe = &eventLoop->events[fd];

    return fe->mask;
}

static void aeGetTime(long *seconds, long *milliseconds)
{
    struct timeval tv;

    gettimeofday(&tv, NULL);
    *seconds = tv.tv_sec;
    *milliseconds = tv.tv_usec/1000;
}

static void aeAddMillisecondsToNow(long long milliseconds, long *sec, long *ms) {
    long cur_sec, cur_ms, when_sec, when_ms;

    aeGetTime(&cur_sec, &cur_ms);
    when_sec = cur_sec + milliseconds/1000;
    when_ms = cur_ms + milliseconds%1000;
    if (when_ms >= 1000) {
        when_sec ++;
        when_ms -= 1000;
    }
    *sec = when_sec;
    *ms = when_ms;
}

long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds,
        aeTimeProc *proc, void *clientData,
        aeEventFinalizerProc *finalizerProc)
{
    long long id = eventLoop->timeEventNextId++;
    aeTimeEvent *te;

    te = zmalloc(sizeof(*te));
    if (te == NULL) return AE_ERR;
    te->id = id;
    aeAddMillisecondsToNow(milliseconds,&te->when_sec,&te->when_ms);
    te->timeProc = proc;
    te->finalizerProc = finalizerProc;
    te->clientData = clientData;
    te->next = eventLoop->timeEventHead;
    eventLoop->timeEventHead = te;
    return id;
}

int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id)
{
    aeTimeEvent *te = eventLoop->timeEventHead;
    while(te) {
        if (te->id == id) {
            te->id = AE_DELETED_EVENT_ID;
            return AE_OK;
        }
        te = te->next;
    }
    return AE_ERR; /* NO event with the specified ID found */
}

/* Search the first timer to fire.
 * This operation is useful to know how many time the select can be
 * put in sleep without to delay any event.
 * If there are no timers NULL is returned.
 *
 * Note that's O(N) since time events are unsorted.
 * Possible optimizations (not needed by Redis so far, but...):
 * 1) Insert the event in order, so that the nearest is just the head.
 *    Much better but still insertion or deletion of timers is O(N).
 * 2) Use a skiplist to have this operation as O(1) and insertion as O(log(N)).
 */
static aeTimeEvent *aeSearchNearestTimer(aeEventLoop *eventLoop)
{
    aeTimeEvent *te = eventLoop->timeEventHead;
    aeTimeEvent *nearest = NULL;

    while(te) {
        if (!nearest || te->when_sec < nearest->when_sec ||
                (te->when_sec == nearest->when_sec &&
                 te->when_ms < nearest->when_ms))
            nearest = te;
        te = te->next;
    }
    return nearest;
}

/* Process time events */
static int processTimeEvents(aeEventLoop *eventLoop) {
    int processed = 0;
    aeTimeEvent *te, *prev;
    long long maxId;
    time_t now = time(NULL);

    /* If the system clock is moved to the future, and then set back to the
     * right value, time events may be delayed in a random way. Often this
     * means that scheduled operations will not be performed soon enough.
     *
     * Here we try to detect system clock skews, and force all the time
     * events to be processed ASAP when this happens: the idea is that
     * processing events earlier is less dangerous than delaying them
     * indefinitely, and practice suggests it is. */
    if (now < eventLoop->lastTime) {
        te = eventLoop->timeEventHead;
        while(te) {
            te->when_sec = 0;
            te = te->next;
        }
    }
    eventLoop->lastTime = now;

    prev = NULL;
    te = eventLoop->timeEventHead;
    maxId = eventLoop->timeEventNextId-1;
    while(te) {
        long now_sec, now_ms;
        long long id;

        /* Remove events scheduled for deletion. */
        if (te->id == AE_DELETED_EVENT_ID) {
            aeTimeEvent *next = te->next;
            if (prev == NULL)
                eventLoop->timeEventHead = te->next;
            else
                prev->next = te->next;
            if (te->finalizerProc)
                te->finalizerProc(eventLoop, te->clientData);
            zfree(te);
            te = next;
            continue;
        }

        /* Make sure we don't process time events created by time events in
         * this iteration. Note that this check is currently useless: we always
         * add new timers on the head, however if we change the implementation
         * detail, this check may be useful again: we keep it here for future
         * defense. */
        if (te->id > maxId) {
            te = te->next;
            continue;
        }
        aeGetTime(&now_sec, &now_ms);
        if (now_sec > te->when_sec ||
            (now_sec == te->when_sec && now_ms >= te->when_ms))
        {
            int retval;

            id = te->id;
            retval = te->timeProc(eventLoop, id, te->clientData);
            processed++;
            if (retval != AE_NOMORE) {
                aeAddMillisecondsToNow(retval,&te->when_sec,&te->when_ms);
            } else {
                te->id = AE_DELETED_EVENT_ID;
            }
        }
        prev = te;
        te = te->next;
    }
    return processed;
}

/* Process every pending time event, then every pending file event
 * (that may be registered by time event callbacks just processed).
 * Without special flags the function sleeps until some file event
 * fires, or when the next time event occurs (if any).
 *
 * If flags is 0, the function does nothing and returns.
 * if flags has AE_ALL_EVENTS set, all the kind of events are processed.
 * if flags has AE_FILE_EVENTS set, file events are processed.
 * if flags has AE_TIME_EVENTS set, time events are processed.
 * if flags has AE_DONT_WAIT set the function returns ASAP until all
 * if flags has AE_CALL_AFTER_SLEEP set, the aftersleep callback is called.
 * the events that's possible to process without to wait are processed.
 *
 * The function returns the number of events processed. */
int aeProcessEvents(aeEventLoop *eventLoop, int flags)
{
    int processed = 0, numevents;
    //printf("ae.c/aeProcessEvents @@@@@@\n");

    /* Nothing to do? return ASAP */
    if (!(flags & AE_TIME_EVENTS) && !(flags & AE_FILE_EVENTS)) return 0;

    /* Note that we want call select() even if there are no
     * file events to process as long as we want to process time
     * events, in order to sleep until the next time event is ready
     * to fire. */
    if (eventLoop->maxfd != -1 ||
        ((flags & AE_TIME_EVENTS) && !(flags & AE_DONT_WAIT))) {
        int j;
        aeTimeEvent *shortest = NULL;
        struct timeval tv, *tvp;

        if (flags & AE_TIME_EVENTS && !(flags & AE_DONT_WAIT))
            shortest = aeSearchNearestTimer(eventLoop);
        if (shortest) {
            long now_sec, now_ms;

            aeGetTime(&now_sec, &now_ms);
            tvp = &tv;

            /* How many milliseconds we need to wait for the next
             * time event to fire? */
            long long ms =
                (shortest->when_sec - now_sec)*1000 +
                shortest->when_ms - now_ms;

            if (ms > 0) {
                tvp->tv_sec = ms/1000;
                tvp->tv_usec = (ms % 1000)*1000;
            } else {
                tvp->tv_sec = 0;
                tvp->tv_usec = 0;
            }
        } else {
            /* If we have to check for events but need to return
             * ASAP because of AE_DONT_WAIT we need to set the timeout
             * to zero */
            if (flags & AE_DONT_WAIT) {
                tv.tv_sec = tv.tv_usec = 0;
                tvp = &tv;
            } else {
                /* Otherwise we can block */
                tvp = NULL; /* wait forever */
            }
        }

        /* Call the multiplexing API, will return only on timeout or when
         * some event fires. */
        //numevents = aeApiPoll(eventLoop, tvp);
        //////////////////////////////////////////////////////////////////////////////////////////////////
        /* _JL_ multiplexing here */
        //zeus_sgarray sga;
        int ii, jj;
        int qtoken_index = 0;
        int sga_index = 0;
        struct qd_status *qd_status_iterptr;
        // set numevents to 0, and then redis event mechanism will not be triggered
        numevents = 0;
        //zeus_sgarray *sga_ptr = malloc(sizeof(zeus_sgarray));
        //zeus_sgarray *sga_ptr = &(eventLoop->sgarray_list[0]);
        zeus_sgarray *sga_ptr;
        for(qd_status_iterptr = eventLoop->qd_status_map; qd_status_iterptr != NULL;
                qd_status_iterptr = qd_status_iterptr->hh.next){
            if((qd_status_iterptr->status_token_arr)[0] == LIBOS_Q_STATUS_listen_nopop){
                // qd is listening and pop not called
                sga_ptr = use_sgarray(eventLoop);
                zeus_qtoken qt = zeus_pop(qd_status_iterptr->qd, sga_ptr);
                //fprintf(stderr, "after zeus_pop qt:%lu\n", qt);
                if(qt == 0){
                    // we could call accept here if qt == 0
                    acceptTcpHandler(eventLoop, qd_status_iterptr->qd, NULL, 0);
                    (qd_status_iterptr->status_token_arr)[0] = LIBOS_Q_STATUS_listen_nopop;
                    processed++;
                    qtoken_index = 0;
                    break;
                }else{
                    //fprintf(stderr, "aeProcessEvents, listen qd:%d from nonpop to inwait real_fd:%d\n", qd_status_iterptr->qd, zeus_qd2fd(qd_status_iterptr->qd));
                    // save the qt for accept
                    qd_status_iterptr->status_token_arr[1] = qt;
                    qd_status_iterptr->status_token_arr[0] = LIBOS_Q_STATUS_listen_inwait;
                    // enqueue the qtoken
                    eventLoop->wait_qtokens[qtoken_index] = qt;
                    qtoken_index++;
                    continue;
                }
            }
            if((qd_status_iterptr->status_token_arr)[0] == LIBOS_Q_STATUS_listen_inwait){
                zeus_qtoken qt = (qd_status_iterptr->status_token_arr)[1];
                eventLoop->wait_qtokens[qtoken_index] = qt;
                qtoken_index++;
                continue;
            }
            if((qd_status_iterptr->status_token_arr)[0] == LIBOS_Q_STATUS_read_nopop){
                //fprintf(stderr, "before pop for read qd:%d\n", qd_status_iterptr->qd);
                sga_ptr = use_sgarray(eventLoop);
                zeus_qtoken qt = zeus_pop(qd_status_iterptr->qd, sga_ptr);
                //fprintf(stderr, "after pop for read qt:%lu\n", qt);
                if(qt == 0){
                    zeus_qtoken client_addr = qd_status_iterptr->status_token_arr[2];
                    client *c = (client*)(client_addr);
                    if(c == NULL){
                        fprintf(stderr, "ERROR, client not created for qd:%d\n",
                                qd_status_iterptr->qd);
                    }
                    c->sga_ptr = sga_ptr;
                    //sga_ptr++;
                    readQueryFromClient(eventLoop, qd_status_iterptr->qd, c, 0);
                    (qd_status_iterptr->status_token_arr)[0] = LIBOS_Q_STATUS_read_nopop;
                    qtoken_index = 0;
                    processed++;
                    break;
                }else{
                    // save the qt for read
                    qd_status_iterptr->status_token_arr[1] = qt;
                    qd_status_iterptr->status_token_arr[0] = LIBOS_Q_STATUS_read_inwait;
                    // enqueue qtoken
                    eventLoop->wait_qtokens[qtoken_index] = qt;
                    qtoken_index++;
                    continue;
                }
            }

            if((qd_status_iterptr->status_token_arr)[0] == LIBOS_Q_STATUS_read_inwait){
                //fprintf(stderr, "LIBOS_Q_STATUS_read_inwait qd:%d\n",qd_status_iterptr->qd);
                zeus_qtoken qt = (qd_status_iterptr->status_token_arr)[1];
                eventLoop->wait_qtokens[qtoken_index] = qt;
                qtoken_index++;
                continue;
            }
        }
        for(ii = 0; ii < qtoken_index; ii++){
            //fprintf(stderr, "qtoken_index:%d qtoken is:%lu\n", ii, eventLoop->wait_qtokens[ii]);
        }
        // now wait_any
        if(qtoken_index > 0){
            int ret_offset = -1, ret_qd = 0;
            sga_ptr = use_sgarray(eventLoop);
            ssize_t ret = zeus_wait_any(eventLoop->wait_qtokens, qtoken_index, &ret_offset, &ret_qd, sga_ptr);
            //fprintf(stderr, "waitany return qd:%d\n", ret_qd);
            struct qd_status *ret_qd_status = find_queue_status_item(eventLoop, ret_qd);
            if(ret_qd_status == NULL){
                fprintf(stderr, "ERROR ret_qd_status is NULL for qd:%d\n", ret_qd);
                exit(1);
            }
            if(ret_qd_status->status_token_arr[0] == LIBOS_Q_STATUS_listen_inwait){
               // fprintf(stderr, "aeProcessEvents wait return qd:%d status is listen_inwait\n", ret_qd);
                acceptTcpHandler(eventLoop, ret_qd_status->qd, NULL, 0);
                (ret_qd_status->status_token_arr)[0] = LIBOS_Q_STATUS_listen_nopop;
            }else if(ret_qd_status->status_token_arr[0] == LIBOS_Q_STATUS_read_inwait){
                //fprintf(stderr, "aeProcessEvents wait return qd:%d status is read_inwait\n", ret_qd);
                zeus_qtoken client_addr = ret_qd_status->status_token_arr[2];
                client *c = (client*)(client_addr);
                if(c == NULL){
                    fprintf(stderr, "ERROR, client not created for qd:%d\n",
                            ret_qd_status->qd);
                }
                c->sga_ptr = sga_ptr;
                //sga_ptr++;
                readQueryFromClient(eventLoop, ret_qd_status->qd, c, 0);
                (ret_qd_status->status_token_arr)[0] = LIBOS_Q_STATUS_read_nopop;
            }else{
                // ERROR
            }
        }
        //////////////////////////////////////////////////////////////////////////////////////////////////

        /* After sleep callback. */
        if (eventLoop->aftersleep != NULL && flags & AE_CALL_AFTER_SLEEP)
            eventLoop->aftersleep(eventLoop);

        for (j = 0; j < numevents; j++) {
            aeFileEvent *fe = &eventLoop->events[eventLoop->fired[j].fd];
            fprintf(stderr, "fd:%d\n", eventLoop->fired[j].fd);
            int mask = eventLoop->fired[j].mask;
            int fd = eventLoop->fired[j].fd;
            int qd = eventLoop->fired[j].qd;
            int fired = 0; /* Number of events fired for current fd. */

            /* Normally we execute the readable event first, and the writable
             * event laster. This is useful as sometimes we may be able
             * to serve the reply of a query immediately after processing the
             * query.
             *
             * However if AE_BARRIER is set in the mask, our application is
             * asking us to do the reverse: never fire the writable event
             * after the readable. In such a case, we invert the calls.
             * This is useful when, for instance, we want to do things
             * in the beforeSleep() hook, like fsynching a file to disk,
             * before replying to a client. */
            int invert = fe->mask & AE_BARRIER;

	        /* Note the "fe->mask & mask & ..." code: maybe an already
             * processed event removed an element that fired and we still
             * didn't processed, so we check if the event is still valid.
             *
             * Fire the readable event if the call sequence is not
             * inverted. */
            if (!invert && fe->mask & mask & AE_READABLE) {
                fe->rfileProc(eventLoop,qd,fe->clientData,mask);
                fired++;
            }

            /* Fire the writable event. */
            if (fe->mask & mask & AE_WRITABLE) {
                if (!fired || fe->wfileProc != fe->rfileProc) {
                    fe->wfileProc(eventLoop,qd,fe->clientData,mask);
                    fired++;
                }
            }

            /* If we have to invert the call, fire the readable event now
             * after the writable one. */
            if (invert && fe->mask & mask & AE_READABLE) {
                if (!fired || fe->wfileProc != fe->rfileProc) {
                    fe->rfileProc(eventLoop,qd,fe->clientData,mask);
                    fired++;
                }
            }

            processed++;
        }
    }
    /* Check time events */
    if (flags & AE_TIME_EVENTS)
        processed += processTimeEvents(eventLoop);
    return processed; /* return the number of processed file/time events */
}

/* Wait for milliseconds until the given file descriptor becomes
 * writable/readable/exception */
int aeWait(int fd, int mask, long long milliseconds) {
    struct pollfd pfd;
    int retmask = 0, retval;

    memset(&pfd, 0, sizeof(pfd));
    pfd.fd = fd;
    if (mask & AE_READABLE) pfd.events |= POLLIN;
    if (mask & AE_WRITABLE) pfd.events |= POLLOUT;

    if ((retval = poll(&pfd, 1, milliseconds))== 1) {
        if (pfd.revents & POLLIN) retmask |= AE_READABLE;
        if (pfd.revents & POLLOUT) retmask |= AE_WRITABLE;
	if (pfd.revents & POLLERR) retmask |= AE_WRITABLE;
        if (pfd.revents & POLLHUP) retmask |= AE_WRITABLE;
        return retmask;
    } else {
        return retval;
    }
}

void aeMain(aeEventLoop *eventLoop) {
    eventLoop->stop = 0;
    int ret;
    while (!eventLoop->stop) {
        if (eventLoop->beforesleep != NULL)
            eventLoop->beforesleep(eventLoop);
        if(ret > 2){
            nanosleep((const struct timespec[]){{0, 1L}}, NULL);
        }
        ret = aeProcessEvents(eventLoop, AE_ALL_EVENTS|AE_CALL_AFTER_SLEEP);
    }
}

char *aeGetApiName(void) {
    return aeApiName();
}

void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep) {
    eventLoop->beforesleep = beforesleep;
}

void aeSetAfterSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *aftersleep) {
    eventLoop->aftersleep = aftersleep;
}
