#include "ydb_server_2pl.h"
#include "extent_client.h"

//#define DEBUG 1

unsigned int hash(char *str);

ydb_server_2pl::ydb_server_2pl(std::string extent_dst, std::string lock_dst) : ydb_server(extent_dst, lock_dst) {
}

ydb_server_2pl::~ydb_server_2pl() {
}

ydb_protocol::status ydb_server_2pl::transaction_begin(int, ydb_protocol::transaction_id &out_id) {    // the first arg is not used, it is just a hack to the rpc lib
	// lab3: your code here
	out_id = crtid++;
	activemap[out_id] = true;
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::transaction_commit(ydb_protocol::transaction_id id, int &) {
	// lab3: your code here
	if(!activemap[id]) {
		return ydb_protocol::TRANSIDINV;
	}
	std::list<lock_protocol::lockid_t> locklist = lockmap[id];
	for(lock_protocol::lockid_t lid : locklist) {
		lc->release(lid);
	}
	activemap[id] = false;
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::transaction_abort(ydb_protocol::transaction_id id, int &) {
	// lab3: your code here
	if(!activemap[id]) {
		return ydb_protocol::TRANSIDINV;
	}
	std::list<lock_protocol::lockid_t> locklist = lockmap[id];
	for(lock_protocol::lockid_t lid : locklist) {
		if(logmap[lid] == id) {
			ec->put(lid, valmap[lid]);
			lc->release(lid);
		}
	}
	activemap[id] = false;
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::get(ydb_protocol::transaction_id id, const std::string key, std::string &out_value) {
	// lab3: your code here
	if(!activemap[id]) {
		return ydb_protocol::TRANSIDINV;
	}
	out_value.clear();
	std::string buf;
	unsigned int eid = hash((char *)key.c_str()) % 1024;
	while(allocmap[eid] == true && keymap[eid] != key) {
		eid++;
	}
	acquire_wrapper(id, eid);
	if(allocmap[eid] == true) {
		ec->get(eid, buf);
		out_value.assign(buf);
	}
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::set(ydb_protocol::transaction_id id, const std::string key, const std::string value, int &) {
	// lab3: your code here
	if(!activemap[id]) {
		return ydb_protocol::TRANSIDINV;
	}
	unsigned int eid = hash((char *)key.c_str()) % 1024;
	while(allocmap[eid] == true && keymap[eid] != key) {
		eid++;
	}
	acquire_wrapper(id, (unsigned long long)eid);
	if(logmap[eid] != id) {
		std::string log_buf;
		ec->get(eid, log_buf);
		valmap[eid] = log_buf;
		logmap[eid] = id;
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

ydb_protocol::status ydb_server_2pl::del(ydb_protocol::transaction_id id, const std::string key, int &) {
	// lab3: your code here
	if(!activemap[id]) {
		return ydb_protocol::TRANSIDINV;
	}
	unsigned int eid = hash((char *)key.c_str()) % 1024;
	while(allocmap[eid] == true && keymap[eid] != key) {
		eid++;
	}
	acquire_wrapper(id, eid);
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

/*unsigned int hash(char *str) {
	unsigned int seed = 131;
    unsigned int hash = 0;

    while(*str)
    {
        hash = hash * seed + *str;
		str++;
    }
 
    return (hash & 0x7FFFFFFF);
}*/

void ydb_server_2pl::acquire_wrapper(ydb_protocol::transaction_id tid, lock_protocol::lockid_t eid) {
	std::list<lock_protocol::lockid_t> locklist = lockmap[tid];
	bool isfound = false;
	for(lock_protocol::lockid_t lid : locklist) {
		if(lid == eid) {
			isfound = true;
			break;
		}
	}
	if(!isfound) {
		locklist.push_back(eid);
		lockmap[tid] = locklist;
		printf("%d acquire: %llu\n", tid, eid);
		lc->acquire(eid);
	}
}
