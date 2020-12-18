// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id)
{
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::create, type, id);
  cache_entry ce;
  if(cache.count(id) == 0) {
    ce = (cache_entry)(new _cache_entry);
    cache[id] = ce;
  }
  else {
    ce = cache[id];
  }
  ce->valid_read = false;
  ce->valid_write = false;
  ce->valid_attr = false;
  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  if(cache.count(eid) != 0) {
    cache_entry ce = cache[eid];
    if(ce->valid_read) {
      buf.assign(ce->content);
    }
    else {
      ret = cl->call(extent_protocol::get, eid, buf);
      ce->valid_read = true;
      ce->content.assign(buf);
    }
  }
  else {
    ret = cl->call(extent_protocol::get, eid, buf);
    cache_entry ce = (cache_entry)(new _cache_entry);
    ce->valid_read = true;
    ce->valid_write = false;
    ce->content.assign(buf);
    cache[eid] = ce;
  }
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  if(cache.count(eid) != 0) {
    cache_entry ce = cache[eid];
    if(ce->valid_attr) {
      attr.atime = ce->attr.atime; attr.ctime = ce->attr.ctime;
      attr.mtime = ce->attr.mtime; attr.size = ce->attr.size;
      attr.type = ce->attr.type;
    }
    else {
      ret = cl->call(extent_protocol::getattr, eid, attr);
      ce->valid_attr = true;
      ce->attr.atime = attr.atime; ce->attr.ctime = attr.ctime;
      ce->attr.mtime = attr.mtime; ce->attr.size = attr.size;
      ce->attr.type = attr.type;
    }
  }
  else {
    ret = cl->call(extent_protocol::getattr, eid, attr);
    cache_entry ce = (cache_entry)(new _cache_entry);
    ce->valid_attr = true;
    ce->attr.atime = attr.atime; ce->attr.ctime = attr.ctime;
    ce->attr.mtime = attr.mtime; ce->attr.size = attr.size;
    ce->attr.type = attr.type;
    cache[eid] = ce;
  }
  
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  int r;
  if(cache.count(eid) != 0) {
    cache_entry ce = cache[eid];
    if(ce->valid_write) {
      ce->content.assign(buf);
    }
    else {
      ret = cl->call(extent_protocol::put, eid, buf, r);
      ce->valid_read = true;
      ce->valid_write = true;
      ce->content.assign(buf);
    }
  }
  else {
    ret = cl->call(extent_protocol::put, eid, buf, r);
    cache_entry ce = (cache_entry)(new _cache_entry);
    ce->valid_read = true;
    ce->valid_write = true;
    ce->content.assign(buf);
    cache[eid] = ce;
  }
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  int r;
  ret = cl->call(extent_protocol::remove, eid, r);
  if(cache.count(eid) != 0) {
    cache_entry ce = cache[eid];
    ce->valid_read = false;
    ce->valid_write = false;
    ce->valid_attr = false;
  }
  return ret;
}
