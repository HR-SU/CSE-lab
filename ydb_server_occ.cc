#include "ydb_server_occ.h"
#include "extent_client.h"

//#define DEBUG 1

unsigned int hash(char *str);

ydb_server_occ::ydb_server_occ(std::string extent_dst, std::string lock_dst) : ydb_server(extent_dst, lock_dst) {
}

ydb_server_occ::~ydb_server_occ() {
}


ydb_protocol::status ydb_server_occ::transaction_begin(int, ydb_protocol::transaction_id &out_id) {    // the first arg is not used, it is just a hack to the rpc lib
	// lab3: your code here
	out_id = ++crtid;
	activemap[out_id] = true;
	transaction_info info = (transaction_info)(new _transaction_info);
	info->start = clock();
	info->valid = LONG_MAX;
	info->finish = LONG_MAX;
	infomap[out_id] = info;
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_occ::transaction_commit(ydb_protocol::transaction_id id, int &) {
	// lab3: your code here
	if(!activemap[id]) {
		return ydb_protocol::TRANSIDINV;
	}
	transaction_info info = infomap[id];
	lc->acquire(1);
	info->valid = clock();
	for(std::map<ydb_protocol::transaction_id, transaction_info>::iterator itr = infomap.begin();
	itr != infomap.end(); itr++) {
		if(itr->first == id) continue;
		if(itr->second->valid < info->valid) {
			if(itr->second->finish > info->valid) {
				std::set<unsigned int> out1;
				std::set_intersection(itr->second->writeset.begin(),
					itr->second->writeset.end(), info->writeset.begin(),
					info->writeset.end(), std::inserter(out1, out1.begin()));
				if(!out1.empty()) {
					info->valid = LONG_MAX;
					lc->release(1);
					int r;
					transaction_abort(id, r);
					return ydb_protocol::ABORT;
				}
			}
			if(itr->second->finish > info->start) {
				std::set<unsigned int> out2;
				std::set_intersection(itr->second->writeset.begin(),
					itr->second->writeset.end(), info->readset.begin(),
					info->readset.end(), std::inserter(out2, out2.begin()));
				if(!out2.empty()) {
					info->valid = LONG_MAX;
					lc->release(1);
					int r;
					transaction_abort(id, r);
					return ydb_protocol::ABORT;
				}
			}
		}
	}
	lc->release(1);
	for(std::set<unsigned int>::iterator itr = info->writeset.begin();
	itr != info->writeset.end(); itr++) {
		ec->put(*itr, info->writelog[*itr]);
	}
	info->finish = clock();
	activemap[id] = false;
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_occ::transaction_abort(ydb_protocol::transaction_id id, int &) {
	// lab3: your code here
	if(!activemap[id]) {
		return ydb_protocol::TRANSIDINV;
	}
	transaction_info info = infomap[id];
	info->valid = LONG_MAX;
	info->finish = LONG_MAX;
	activemap[id] = false;
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_occ::get(ydb_protocol::transaction_id id, const std::string key, std::string &out_value) {
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
	transaction_info info = infomap[id];
	if(info->writeset.count(eid)) {
		out_value.assign(info->writelog[eid]);
	}
	else if(info->readset.count(eid)) {
		out_value.assign(info->readlog[eid]);
	}
	else if(allocmap[eid] == true) {
		ec->get(eid, buf);
		out_value.assign(buf);
	}
	info->readset.insert(eid);
	info->readlog[eid] = out_value;
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_occ::set(ydb_protocol::transaction_id id, const std::string key, const std::string value, int &) {
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
	transaction_info info = infomap[id];
	info->writeset.insert(eid);
	info->writelog[eid] = value;
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_occ::del(ydb_protocol::transaction_id id, const std::string key, int &) {
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
	transaction_info info = infomap[id];
	info->writeset.insert(eid);
	info->writelog[eid] = std::string();
	return ydb_protocol::OK;
}

