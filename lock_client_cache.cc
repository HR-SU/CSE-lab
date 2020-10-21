// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"

#define VERBOSE 0
int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&cond, NULL);
  pthread_cond_init(&cond_retry, NULL);
  retry_ready = false;
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);
}

bool
lock_client_cache::isowner(lock_protocol::lockid_t lid)
{
  if(lockpool[lid] == LOCKED) {
    while(lockpool[lid] != FREE && lockpool[lid] != NONE) {
      pthread_cond_wait(&cond, &mutex);
    }
    if(lockpool[lid] == FREE) {
      lockpool[lid] = LOCKED;
      return true;
    }
  }
  else if(lockpool[lid] == FREE) {
    lockpool[lid] = LOCKED;
    return true;
  }

  return false;
}

lock_protocol::status
lock_client_cache::acquirecall(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;
  int r;
  while(1) {
    lockpool[lid] = ACUIRING;
    pthread_mutex_unlock(&mutex);
    ret = cl->call(lock_protocol::acquire, lid, id, r);
    pthread_mutex_lock(&mutex);
    if(VERBOSE) tprintf("%s(%lu): rpc ret is %d\n", id.c_str(), pthread_self(), ret);
    if(isowner(lid)) {
      return lock_protocol::OK;
    }
    switch(ret) {
      case lock_protocol::RETRY: {
        if(VERBOSE) tprintf("%s(%lu): lock %llu is not available, retry\n", id.c_str(), pthread_self(), lid);
        if(retry_ready == true) {
          if(VERBOSE) tprintf("%s(%lu): retry is already ready on lock %llu\n", id.c_str(), pthread_self(), lid);
          retry_ready = false;
          continue;
        }
        while(retry_ready == false) {
          pthread_cond_wait(&cond_retry, &mutex);
        }
        retry_ready = false;
        if(isowner(lid)) {
          return lock_protocol::OK;
        }
        continue;
      }

      case lock_protocol::OK: {
        if(lockpool[lid] == NONE || lockpool[lid] == RELEASING) {
          if(VERBOSE) tprintf("%s(%lu): lock %lld has already been released, retry\n", id.c_str(), pthread_self(), lid);
          continue;
        }
        lockpool[lid] = LOCKED;
        return lock_protocol::OK;
      }
      
      default: {
        VERIFY (0);
      }
    }
    return ret;
  }
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;
  // Your lab2 part3 code goes here
  pthread_mutex_lock(&mutex);
  if(VERBOSE) tprintf("%s(%lu): trying to acquire lock %llu\n", id.c_str(), pthread_self(), lid);
  if(lockpool.count(lid) == 0) lockpool[lid] = NONE;
  
  switch(lockpool[lid]) {
    case NONE: {
      if(VERBOSE) tprintf("%s(%lu): don't own lock %llu, send rpc to acquire it\n", id.c_str(), pthread_self(), lid);
      ret = acquirecall(lid);
      break;
    }
    case FREE: {
      lockpool[lid] = LOCKED;
      break;
    }
    case LOCKED:
    case ACUIRING:
    case RELEASING: {
      while(lockpool[lid] != FREE && lockpool[lid] != NONE) {
        pthread_cond_wait(&cond, &mutex);
      }
      if(lockpool[lid] == FREE) {
        lockpool[lid] = LOCKED;
      }
      else {
        ret = acquirecall(lid);
      }
      break;
    }
    default: {
      VERIFY(0);
    }
  }
  pthread_mutex_unlock(&mutex);

  return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;
  // Your lab2 part3 code goes here
  pthread_mutex_lock(&mutex);
  if(lockpool[lid] == LOCKED || lockpool[lid] == FREE) {
    lockpool[lid] = FREE;
  }
  else {
    lockpool[lid] = NONE;
  }
  pthread_cond_signal(&cond);
  pthread_mutex_unlock(&mutex);
  return ret;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
  int ret = rlock_protocol::OK;
  // Your lab2 part3 code goes here
  pthread_mutex_lock(&mutex);
  if(VERBOSE) tprintf("%s(%lu): server call revoke on lock %llu\n", id.c_str(), pthread_self(), lid);
  while(lockpool[lid] == LOCKED) {
    pthread_cond_wait(&cond, &mutex);
  }
  if(lockpool[lid] == RELEASING || lockpool[lid] == NONE) {
    if(VERBOSE) tprintf("%s(%lu): lock %llu is already revoked\n", id.c_str(), pthread_self(), lid);
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  if(VERBOSE) tprintf("%s(%lu): call rpc to really release lock %llu\n",id.c_str(), pthread_self(), lid);
  lockpool[lid] = RELEASING;
  pthread_mutex_unlock(&mutex);

  int r;
  ret = cl->call(lock_protocol::release, lid, id, r);
  VERIFY (ret == lock_protocol::OK);

  pthread_mutex_lock(&mutex);
  lockpool[lid] = NONE;
  pthread_cond_signal(&cond);
  pthread_mutex_unlock(&mutex);
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
  int ret = rlock_protocol::OK;
  // Your lab2 part3 code goes here
  pthread_mutex_lock(&mutex);
  if(VERBOSE) tprintf("%s(%lu): server call retry on lock %llu\n",id.c_str(), pthread_self(), lid);
  retry_ready = true;
  pthread_cond_signal(&cond_retry);
  pthread_mutex_unlock(&mutex);
  return ret;
}
