// the extent server implementation

#include "extent_server.h"
#include "rpc.h"
#include "handle.h"
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "lang/verify.h"

#define VERBOSE 1
extent_server::extent_server() 
{
  im = new inode_manager();
}

int extent_server::create(uint32_t type, std::string cid, extent_protocol::extentid_t &id)
{
  // alloc a new inode and return inum
  // printf("extent_server: create inode\n");
  id = im->alloc_inode(type);
  extent_info ei;
  if(extents.count(id) != 0) {
    ei = extents[id];
		ei->writer = cid;
	}
	else {
		ei = (extent_info)(new _extent_info);
    extents[id] = ei;
	}
	if(ei->readers.count(cid) == 0) {
    ei->readers.insert(cid);
  }
  if(VERBOSE) printf("create %llu from %s\n", id, cid.c_str());
  return extent_protocol::OK;
}

int extent_server::put(extent_protocol::extentid_t id, std::string cid, std::string buf, int &r)
{
  id &= 0x7fffffff;
  if(VERBOSE) printf("put %llu from %s\n", id, cid.c_str());
  const char * cbuf = buf.c_str();
  int size = buf.size();
  im->write_file(id, cbuf, size);
  
  extent_info ei;
  if(extents.count(id) != 0) {
    ei = extents[id];
    for(std::set<std::string>::iterator it = ei->readers.begin(); it != ei->readers.end(); it++) {
      std::string reader = *it;
      if(reader == cid) {continue;}
			if(VERBOSE) printf("es: ivalidate cache of eid %llu of %s\n", id, reader.c_str());
      handle hd(reader);
      rpcc *cl = hd.safebind();
      extent_protocol::status ret; int r;
      if(cl) ret = cl->call(extent_protocol::invalidate, id, r);
      else VERIFY(0);
			VERIFY(ret == extent_protocol::OK);
    }
  }
  else {
    ei = (extent_info)(new _extent_info);
    extents[id] = ei;
  }
  ei->writer = cid;
	if(ei->readers.count(cid) == 0) {
    ei->readers.insert(cid);
  }
  r = 0;
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string cid, std::string &buf)
{
  if(VERBOSE) printf("es: get %llu from %s\n", id, cid.c_str());

  id &= 0x7fffffff;

  int size = 0;
  char *cbuf = NULL;
  extent_info ei;
  bool flag = false;
  if(extents.count(id) != 0) {
    ei = extents[id];
    if(ei->writer != cid && ei->writer.size() > 0) {
			if(VERBOSE) printf("es: update content of eid %llu of %s\n", id, ei->writer.c_str());
      handle hd(ei->writer);
      rpcc *cl = hd.safebind();
      extent_protocol::status ret;
      if(cl) ret = cl->call(extent_protocol::update_content, id, buf);
      else VERIFY(0);
			VERIFY(ret == extent_protocol::OK);
    }
    else flag = true;
  }
  else {
    ei = (extent_info)(new _extent_info);
    extents[id] = ei;
    flag = true;
  }
  if(flag) {
    im->read_file(id, &cbuf, &size);
    if (size == 0)
      buf = "";
    else {
      buf.assign(cbuf, size);
      free(cbuf);
    }
  }
  if(ei->readers.count(cid) == 0) {
    ei->readers.insert(cid);
  }

  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, std::string cid, extent_protocol::attr &a)
{
  id &= 0x7fffffff;
  if(VERBOSE) printf("getattr %llu from %s\n", id, cid.c_str());
  extent_protocol::attr attr;
  extent_info ei;
  bool flag = false;
  if(extents.count(id) != 0) {
    ei = extents[id];
    if(ei->writer != cid && ei->writer.size() > 0) {
			if(VERBOSE) printf("es: update attr of eid %llu of %s\n", id, ei->writer.c_str());
      handle hd(ei->writer);
      rpcc *cl = hd.safebind();
      extent_protocol::status ret;
      if(cl) ret = cl->call(extent_protocol::update_attr, id, a);
      else VERIFY(0);
			VERIFY(ret == extent_protocol::OK);
    }
    else flag = true;
  }
  else {
    ei = (extent_info)(new _extent_info);
    extents[id] = ei;
    flag = true;
  }
  if(flag) {
    memset(&attr, 0, sizeof(attr));
    im->getattr(id, attr);
    a = attr;
  }
  if(ei->readers.count(cid) == 0) {
    ei->readers.insert(cid);
  }
  
  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, std::string cid, int &r)
{
  if(VERBOSE) printf("remove %llu from %s\n", id, cid.c_str());
  id &= 0x7fffffff;
  im->remove_file(id);

  extent_info ei;
  if(extents.count(id) != 0) {
    ei = extents[id];
    for(std::set<std::string>::iterator it = ei->readers.begin(); it != ei->readers.end(); it++) {
      std::string reader = *it;
      if(reader == cid) continue;
      handle hd(reader);
      rpcc *cl = hd.safebind();
      extent_protocol::status ret; int r;
      if(cl) ret = cl->call(extent_protocol::invalidate, id, r);
      else VERIFY(0);
			VERIFY(ret == extent_protocol::OK);
    }
  }
  r = 0;
  return extent_protocol::OK;
}

