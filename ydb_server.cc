#include "ydb_server.h"
#include "extent_client.h"

//#define DEBUG 1

unsigned int hash(char *str);

/*static long timestamp(void) {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return (tv.tv_sec*1000 + tv.tv_usec/1000);
}*/

ydb_server::ydb_server(std::string extent_dst, std::string lock_dst) {
	ec = new extent_client(extent_dst);
	lc = new lock_client(lock_dst);
	//lc = new lock_client_cache(lock_dst);
	//long starttime = timestamp();
	for(int i = 2; i < 1024; i++) {    // for simplicity, just pre alloc all the needed inodes
		extent_protocol::extentid_t id;
		ec->create(extent_protocol::T_FILE, id);
		if(id == 0) break;
	}
	crtid = 0;
	std::string buf;
	ec->get(1, buf);
	char *ptr = (char *)buf.c_str();
	unsigned long long left = buf.length();
	unsigned int tmpid, len;
	while(left > 0) {
	    memcpy(&tmpid, ptr, sizeof(unsigned int));
	    ptr += sizeof(unsigned int);
	    memcpy(&len, ptr, sizeof(unsigned int));
	    ptr += sizeof(unsigned int);
	    char *key = new char[len + 1];
	    memcpy(key, ptr, len);
	    ptr += len;
	    key[len] = '\0';
		std::string key_buf;
		key_buf.assign(key, len);
		allocmap[tmpid] = true;
		keymap[tmpid] = key_buf;
	    left -= (2*sizeof(unsigned int) + len);
		delete []key;
	}
	
	//long endtime = timestamp();
	//printf("time %ld ms\n", endtime-starttime);
}

ydb_server::~ydb_server() {
	delete lc;
	delete ec;
}


ydb_protocol::status ydb_server::transaction_begin(int, ydb_protocol::transaction_id &out_id) {    // the first arg is not used, it is just a hack to the rpc lib
	// no imply, just return OK
	out_id = ++crtid;
	activemap[out_id] = true;
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server::transaction_commit(ydb_protocol::transaction_id id, int &) {
	// no imply, just return OK
	if(!activemap[id]) {
		return ydb_protocol::TRANSIDINV;
	}
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server::transaction_abort(ydb_protocol::transaction_id id, int &) {
	// no imply, just return OK
	if(!activemap[id]) {
		return ydb_protocol::TRANSIDINV;
	}
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server::get(ydb_protocol::transaction_id id, const std::string key, std::string &out_value) {
	// lab3: your code here
	if(!activemap[id]) {
		return ydb_protocol::TRANSIDINV;
	}
	out_value.clear();
	std::string buf;
	unsigned int eid = hash((char *)key.c_str());
	while(allocmap[eid] == true && keymap[eid] != key) {
		eid++;
		eid = eid % 1024;
		if(eid < 2) eid = 2;
	}
	if(allocmap[eid] == true) {
		ec->get(eid, buf);
		out_value.assign(buf);
	}
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server::set(ydb_protocol::transaction_id id, const std::string key, const std::string value, int &) {
	// lab3: your code here
	if(!activemap[id]) {
		return ydb_protocol::TRANSIDINV;
	}
	unsigned int eid = hash((char *)key.c_str());
	while(allocmap[eid] == true && keymap[eid] != key) {
		eid++;
		eid = eid % 1024;
		if(eid < 2) eid = 2;
	}
	if(allocmap[eid] == false) {
		allocmap[eid] = true;
		keymap[eid] = key;
		std::string buf;
		ec->get(1, buf);
		const char *keycstr = key.c_str();
		unsigned int len = strlen(keycstr);
		unsigned int tot_len = 2*sizeof(unsigned int) + len;
		char *entry = new char[tot_len];
		memcpy(entry, &eid, sizeof(unsigned int));
		memcpy(entry+sizeof(unsigned int), &len, sizeof(unsigned int));
		memcpy(entry+2*sizeof(unsigned int), keycstr, len);
		std::string buf_tmp;
		buf_tmp.assign(entry, tot_len);
		delete []entry;
		buf.append(buf_tmp);
		ec->put(1, buf);
	}
	ec->put(eid, value);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server::del(ydb_protocol::transaction_id id, const std::string key, int &) {
	// lab3: your code here
	if(!activemap[id]) {
		return ydb_protocol::TRANSIDINV;
	}
	unsigned int eid = hash((char *)key.c_str());
	while(allocmap[eid] == true && keymap[eid] != key) {
		eid++;
		eid = eid % 1024;
		if(eid < 2) eid = 2;
	}
	if(allocmap[eid] == true) {
		allocmap[eid] = false;
		keymap[eid] = std::string();
		std::string buf;
		ec->get(1, buf);
		const char *c_str = buf.c_str();
		char *ptr = (char *)c_str;
		unsigned long long left = buf.length();
		unsigned int keylen = key.length();
		unsigned long long new_length = left - (2*sizeof(unsigned int) + keylen);
		char *ptr_new = new char[new_length];
		unsigned int tmpid, len;
		while(left > 0) {
		    memcpy(&tmpid, ptr, sizeof(unsigned int));
		    ptr += sizeof(unsigned int);
		    memcpy(&len, ptr, sizeof(unsigned int));
		    ptr += sizeof(unsigned int);
		    char *key_buf = new char[len + 1];
		    memcpy(key_buf, ptr, len);
		    ptr += len;
		    key_buf[len] = '\0';
		    if(tmpid != eid) {
		        memcpy(ptr_new, &tmpid, sizeof(unsigned int));
		        ptr_new += sizeof(unsigned int);
		        memcpy(ptr_new, &len, sizeof(unsigned int));
		        ptr_new += sizeof(unsigned int);
		        memcpy(ptr_new, key_buf, len);
		        ptr_new += len;
		    }
		    left -= (2*sizeof(unsigned int) + len);
			delete []key_buf;
		}
		std::string buf_new;
		buf_new.assign(ptr_new, new_length);
		delete []ptr_new;
		ec->put(1, buf_new);
	}
	return ydb_protocol::OK;
}

unsigned int hash(char *str) {
	unsigned int seed = 131;
    unsigned int hash = 0;

    while(*str)
    {
        hash = hash * seed + *str;
		str++;
    }
 
    return ((hash&0x7FFFFFFF)%1022)+2;
}
