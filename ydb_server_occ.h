#ifndef ydb_server_occ_h
#define ydb_server_occ_h

#include <string>
#include <map>
#include <list>
#include <set>
#include <time.h>
#include <algorithm>
#include <climits>
#include "extent_client.h"
#include "lock_protocol.h"
#include "lock_client.h"
#include "lock_client_cache.h"
#include "ydb_protocol.h"
#include "ydb_server.h"


class ydb_server_occ: public ydb_server {
protected:
	struct _transaction_info {
		clock_t start, valid, finish;
		std::set<unsigned int> readset;
		std::set<unsigned int> writeset;
		std::map<unsigned int, std::string> writelog;
	};
	typedef _transaction_info *transaction_info;
	std::map<ydb_protocol::transaction_id, transaction_info> infomap;
public:
	ydb_server_occ(std::string, std::string);
	~ydb_server_occ();
	ydb_protocol::status transaction_begin(int, ydb_protocol::transaction_id &);
	ydb_protocol::status transaction_commit(ydb_protocol::transaction_id, int &);
	ydb_protocol::status transaction_abort(ydb_protocol::transaction_id, int &);
	ydb_protocol::status get(ydb_protocol::transaction_id, const std::string, std::string &);
	ydb_protocol::status set(ydb_protocol::transaction_id, const std::string, const std::string, int &);
	ydb_protocol::status del(ydb_protocol::transaction_id, const std::string, int &);
};

#endif

