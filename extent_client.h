// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include "extent_protocol.h"
#include "extent_server.h"

class extent_client {
 private:
  rpcc *cl;
  std::string hostname;
  std::string id;
  int port;

  struct _cache_entry {
    std::string content;
    bool valid_read;
    bool valid_write;
    bool valid_attr;
    extent_protocol::attr attr;
  };
  typedef _cache_entry* cache_entry;
  std::map<extent_protocol::extentid_t, cache_entry> cache;
 public:
  static int last_port;
  extent_client(std::string dst);

  extent_protocol::status create(uint32_t type, extent_protocol::extentid_t &eid);
  extent_protocol::status get(extent_protocol::extentid_t eid, 
			                        std::string &buf);
  extent_protocol::status getattr(extent_protocol::extentid_t eid, 
				                          extent_protocol::attr &a);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
  extent_protocol::status remove(extent_protocol::extentid_t eid);
  
  int invalidate(extent_protocol::extentid_t eid, int &);
  int update_content(extent_protocol::extentid_t eid, std::string &buf);
  int update_attr(extent_protocol::extentid_t eid, extent_protocol::attr &a);
};

#endif