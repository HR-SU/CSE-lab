// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

int extent_client::last_port = 0;

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
  port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << port;
  id = host.str();
  last_port = port;
  rpcs *rlsrpc = new rpcs(port);
  rlsrpc->reg(extent_protocol::update_content, this, &extent_client::update_content);
  rlsrpc->reg(extent_protocol::update_attr, this, &extent_client::update_attr);
  rlsrpc->reg(extent_protocol::invalidate, this, &extent_client::invalidate);
}

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::create, type, id, eid);
  cache_entry ce;
  if(cache.count(eid) == 0) {
    ce = (cache_entry)(new _cache_entry);
    cache[eid] = ce;
  }
  else {
    ce = cache[eid];
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
      ret = cl->call(extent_protocol::get, eid, id, buf);
      ce->valid_read = true;
      ce->content.assign(buf);
    }
		if(ce->valid_attr) {
			ce->attr.atime = time(NULL);
		}
  }
  else {
    ret = cl->call(extent_protocol::get, eid, id, buf);
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
      ret = cl->call(extent_protocol::getattr, eid, id, attr);
      ce->valid_attr = true;
      ce->attr.atime = attr.atime; ce->attr.ctime = attr.ctime;
      ce->attr.mtime = attr.mtime; ce->attr.size = attr.size;
      ce->attr.type = attr.type;
    }
  }
  else {
    ret = cl->call(extent_protocol::getattr, eid, id, attr);
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
      ret = cl->call(extent_protocol::put, eid, id, buf, r);
      ce->valid_read = true;
      ce->valid_write = true;
      ce->content.assign(buf);
    }
		if(ce->valid_attr) {
			ce->attr.size = buf.size();
			ce->attr.mtime = time(NULL); ce->attr.atime = time(NULL);
			ce->attr.ctime = time(NULL);
		}
  }
  else {
    ret = cl->call(extent_protocol::put, eid, id, buf, r);
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
  ret = cl->call(extent_protocol::remove, eid, id, r);
  if(cache.count(eid) != 0) {
    cache_entry ce = cache[eid];
    ce->valid_read = false;
    ce->valid_write = false;
    ce->valid_attr = false;
  }
  return ret;
}

int
extent_client::update_content(extent_protocol::extentid_t eid, std::string &buf)
{
  if(cache.count(eid) != 0) {
    cache_entry ce = cache[eid];
    buf.assign(ce->content);
  }
  return extent_protocol::OK;
}

int
extent_client::update_attr(extent_protocol::extentid_t eid, extent_protocol::attr &a)
{
  if(cache.count(eid) != 0) {
    cache_entry ce = cache[eid];
    a.atime = ce->attr.atime; a.ctime = ce->attr.ctime;
    a.mtime = ce->attr.mtime; a.size = ce->attr.size;
    a.type = ce->attr.type;
  }
  return extent_protocol::OK;
}

int 
extent_client::invalidate(extent_protocol::extentid_t eid, int &r) {
  if(cache.count(eid) != 0) {
    cache_entry ce = cache[eid];
    ce->valid_attr = false;
    ce->valid_read = false;
    ce->valid_write = false;
  }
  return extent_protocol::OK;
}