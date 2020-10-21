#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include <map>
#include <queue>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"


class lock_server_cache {
 private:
  int nacquire;
  enum lock_stat {FREE = 0, LOCKED, REVOKING};
  std::map<lock_protocol::lockid_t, lock_stat> lockpool;
  std::map<lock_protocol::lockid_t, std::string> lockowner;
  std::map<lock_protocol::lockid_t, std::queue<std::string> > lockwaiter;
  pthread_mutex_t mutex;
  
 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
