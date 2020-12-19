// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"


lock_server_cache::lock_server_cache()
{
  pthread_mutex_init(&mutex, NULL);
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, int &)
{
  lock_protocol::status ret = lock_protocol::OK;
  // Your lab2 part3 code goes here
  pthread_mutex_lock(&mutex);
  // tprintf("%s: trying to acquire lock %llu\n", id.c_str(), lid);
  if(lockpool.count(lid) == 0 || lockpool[lid] == FREE) {
    // tprintf("%s: lock %llu is free, grant it directly\n", id.c_str(), lid);
    lockpool[lid] = LOCKED;
    lockowner[lid] = id;
    pthread_mutex_unlock(&mutex);
  }
  else if(lockwaiter.count(lid) == 0 || lockwaiter[lid].empty() 
  || id == lockwaiter[lid].front()) {
    tprintf("%s: lock %llu is owned by %s, no other waiting client, call revoke\n", id.c_str(), lid, lockowner[lid].c_str());
    if(lockowner[lid] == id && lockpool[lid] != REVOKING) {
      pthread_mutex_unlock(&mutex);
      return ret;
    }
    if(lockwaiter.count(lid) == 0 || lockwaiter[lid].empty()) {
      lockwaiter[lid].push(id);
    }
    lockpool[lid] = REVOKING;
    handle hd(lockowner[lid]);
    rlock_protocol::status rret = rlock_protocol::OK;
    int r;
    pthread_mutex_unlock(&mutex);
    rpcc *cl = hd.safebind();
    if(cl){
      rret = cl->call(rlock_protocol::revoke, lid, r);
    } else {
      VERIFY(0);
    }
    VERIFY (rret == rlock_protocol::OK);
    pthread_mutex_lock(&mutex);
    lockpool[lid] = LOCKED;
    lockowner[lid] = id;
    lockwaiter[lid].pop();
    if(!lockwaiter[lid].empty()) {
      tprintf("%s: call %s to retry lock %llu\n", id.c_str(), lockwaiter[lid].front().c_str(), lid);
      handle hdr(lockwaiter[lid].front());
      rret = rlock_protocol::OK;
      pthread_mutex_unlock(&mutex);
      rpcc *cl = hdr.safebind();
      if(cl){
        rret = cl->call(rlock_protocol::retry, lid, r);
      } else {
        VERIFY(0);
      }
      VERIFY (rret == rlock_protocol::OK);
    }
    else {
      pthread_mutex_unlock(&mutex);
    }
  }
  else {
    tprintf("%s: lock %llu is owned by %s with other waiting client, return RETRY\n", id.c_str(), lid, lockowner[lid].c_str());
    lockwaiter[lid].push(id);
    ret = lock_protocol::RETRY;
    pthread_mutex_unlock(&mutex);
  }

  return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  // Your lab2 part3 code goes here
  pthread_mutex_lock(&mutex);
  tprintf("%s: trying to release lock %llu\n", id.c_str(), lid);
  if(lockpool[lid] != REVOKING && lockowner[lid] == id) {
    tprintf("%s: lockpool[%llu] is %d\n", id.c_str(), lid, lockpool[lid]);
    lockpool[lid] = FREE;
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}
