// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client(extent_dst);
    // Lab2: Use lock_client_cache when you test lock_cache
    // lc = new lock_client(lock_dst);
    lc = new lock_client_cache(lock_dst);
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

extent_protocol::types
yfs_client::filetype(inum inum, fileinfo &fin)
{
    extent_protocol::attr a;

	lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return (extent_protocol::types)0;
    }
    lc->release(inum);

    if (a.type == extent_protocol::T_DIR) {
        fin.atime = a.atime;
        fin.mtime = a.mtime;
        fin.ctime = a.ctime;
    }
    else {
        fin.atime = a.atime;
        fin.mtime = a.mtime;
        fin.ctime = a.ctime;
        fin.size = a.size;
    }

    return (extent_protocol::types)a.type;
}
/*
bool
yfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        lc->release(inum);
        printf("error getting attr\n");
        return false;
    }
    lc->release(inum);

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 

    printf("isfile: %lld is not a file\n", inum);
    return false;
}
*/
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */
/*
bool
yfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    extent_protocol::attr a;

    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        lc->release(inum);
        printf("error getting attr\n");
        return false;
    }
    lc->release(inum);

    if (a.type == extent_protocol::T_DIR) {
        printf("isdir: %lld is a directory\n", inum);
        return true;
    } 

    printf("isdir: %lld is not a directory\n", inum);
    return false;
}

bool
yfs_client::issymlink(inum inum)
{
    extent_protocol::attr a;

    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        lc->release(inum);
        printf("error getting attr\n");
        return false;
    }
    lc->release(inum);

    if (a.type == extent_protocol::T_SLINK) {
        printf("issymlink: %lld is a symbol link\n", inum);
        return true;
    } 
    printf("issymlink: %lld is not a symbol link\n", inum);
    return false;
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;

    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        lc->release(inum);
        printf("yfs_client: getdir: IOERR\n");
        r = IOERR;
        goto release;
    }
    lc->release(inum);

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;

    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        lc->release(inum);
        printf("yfs_client: getdir: IOERR\n");
        r = IOERR;
        goto release;
    }
    lc->release(inum);

    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}

int
yfs_client::getsymlink(inum inum, symlinkinfo &slin)
{
    int r = OK;

    std::string buf;
    printf("getsymlink %016llx\n", inum);
    extent_protocol::attr a;

    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        lc->release(inum);
        printf("yfs_client: getdir: IOERR\n");
        r = IOERR;
        goto release;
    }
    
    if(ec->get(inum, buf) != extent_protocol::OK) {
        lc->release(inum);
        r = NOENT;
        goto release;
    }

    lc->release(inum);

    slin.atime = a.atime;
    slin.mtime = a.mtime;
    slin.ctime = a.ctime;
    slin.size = strlen(buf.c_str());
    printf("getsymlink %016llx -> sz %llu\n", inum, slin.size);

release:
    return r;
}
*/

#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    extent_protocol::attr a;
    std::string buf;
    
    lc->acquire(ino);
    ec->getattr(ino, a);
    ec->get(ino, buf);
    size_t old_size = a.size;
    if(size <= old_size) {
        buf = buf.substr(0, size);
    }
    else {
        buf = buf.append(size-old_size, '\0'); 
    }
    ec->put(ino, buf);
    lc->release(ino);

    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool found = false;
    lc->acquire(parent);
    std::string buf;
    ec->get(parent, buf);
    inum id;
    unsigned long long length;
    const char *c_str = buf.c_str();
    unsigned long long left = buf.length();
    char *ptr = (char *)c_str;
    while(left > 0) {
        memcpy(&id, ptr, sizeof(inum));
        ptr += sizeof(inum);
        memcpy(&length, ptr, sizeof(unsigned long long));
        ptr += sizeof(unsigned long long);
        char *name_buf = new char[length + 1];
        memcpy(name_buf, ptr, length);
        name_buf[length] = '\0';
        if(strcmp(name_buf, name) == 0) {
            ino_out = id;
            found = true;
            delete []name_buf;
            break;
        }
        delete []name_buf;
        ptr += length;
        left -= (sizeof(inum) + sizeof(unsigned long long) + length);
    }
    if(found == true) {
        lc->release(parent);
        return EXIST;
    }

    if(ec->create(extent_protocol::T_FILE, id) != extent_protocol::OK) {
        lc->release(parent);
        printf("error when creating file!\n");
    }
    ino_out = id;

    length = strlen(name);
    unsigned long long tol_len = sizeof(inum) + sizeof(unsigned long long) + length;
    char *entry = new char[tol_len];
    memcpy(entry, &id, sizeof(inum));
    memcpy(entry + sizeof(inum), &length, sizeof(unsigned long long));
    memcpy(entry + sizeof(inum) + sizeof(unsigned long long), name, length);
    std::string buf_tmp;
    buf_tmp.assign(entry, tol_len);
    delete []entry;
    buf = buf.append(buf_tmp);
    ec->put(parent, buf);
    lc->release(parent);
    return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool found = false;
    lc->acquire(parent);
    lookup_nolock(parent, name, found, ino_out);
    if(found == true) {
        lc->release(parent);
        return EXIST;
    }

    inum id;
    if(ec->create(extent_protocol::T_DIR, id) != extent_protocol::OK) {
        lc->release(parent);
        printf("error when creating directory!\n");
    }
    ino_out = id;
    
    std::string buf;
    ec->get(parent, buf);
    unsigned long long length = strlen(name);
    unsigned long long tol_len = sizeof(inum) + sizeof(unsigned long long) + length;
    char *entry = new char[tol_len];
    memcpy(entry, &id, sizeof(inum));
    memcpy(entry + sizeof(inum), &length, sizeof(unsigned long long));
    memcpy(entry + sizeof(inum) + sizeof(unsigned long long), name, length);
    std::string buf_tmp;
    buf_tmp.assign(entry, tol_len);
    delete []entry;
    buf = buf.append(buf_tmp);
    ec->put(parent, buf);
    lc->release(parent);
    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    found = false;
    std::string buf;
    lc->acquire(parent);
    ec->get(parent, buf);
    inum id;
    unsigned long long length;
    const char *c_str = buf.c_str();
    unsigned long long left = buf.length();
    char *ptr = (char *)c_str;
    while(left > 0) {
        memcpy(&id, ptr, sizeof(inum));
        ptr += sizeof(inum);
        memcpy(&length, ptr, sizeof(unsigned long long));
        ptr += sizeof(unsigned long long);
        char *name_buf = new char[length + 1];
        memcpy(name_buf, ptr, length);
        name_buf[length] = '\0';
        if(strcmp(name_buf, name) == 0) {
            ino_out = id;
            found = true;
            delete []name_buf;
            break;
        }
        delete []name_buf;
        ptr += length;
        left -= (sizeof(inum) + sizeof(unsigned long long) + length);
    }
    lc->release(parent);
    return r;
}

int
yfs_client::lookup_nolock(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;
    found = false;
    std::string buf;
    ec->get(parent, buf);
    inum id;
    unsigned long long length;
    const char *c_str = buf.c_str();
    unsigned long long left = buf.length();
    char *ptr = (char *)c_str;
    while(left > 0) {
        memcpy(&id, ptr, sizeof(inum));
        ptr += sizeof(inum);
        memcpy(&length, ptr, sizeof(unsigned long long));
        ptr += sizeof(unsigned long long);
        char *name_buf = new char[length + 1];
        memcpy(name_buf, ptr, length);
        name_buf[length] = '\0';
        if(strcmp(name_buf, name) == 0) {
            ino_out = id;
            found = true;
            delete []name_buf;
            break;
        }
        delete []name_buf;
        ptr += length;
        left -= (sizeof(inum) + sizeof(unsigned long long) + length);
    }
    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    std::string buf;
    lc->acquire(dir);
    ec->get(dir, buf);
    inum id;
    unsigned long long length;
    const char *c_str = buf.c_str();
    unsigned long long left = buf.length();
    char *ptr = (char *)c_str;
    while(left > 0) {
        memcpy(&id, ptr, sizeof(inum));
        ptr += sizeof(inum);
        memcpy(&length, ptr, sizeof(unsigned long long));
        ptr += sizeof(unsigned long long);
        char *name_buf = new char[length];
        memcpy(name_buf, ptr, length);
        ptr += length;
        std::string name;
        name.assign((const char *)name_buf, length);
        delete []name_buf;
        struct dirent dirent_buf;
        dirent_buf.inum = id;
        dirent_buf.name = name;
        left -= (sizeof(inum) + sizeof(unsigned long long) + length);
        list.push_back(dirent_buf);
    }
    lc->release(dir);
    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    std::string buf;
    lc->acquire(ino);
    ec->get(ino, buf);
    lc->release(ino);
    unsigned long long total_size = buf.size();
    if((unsigned long long)off >= total_size) {
        data.clear();
        return r;
    }
    if(total_size < off + size) {
        size = total_size - off;
    }
    data.assign(buf.substr(off, size).c_str(), size);
    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    std::string buf;
    lc->acquire(ino);
    ec->get(ino, buf);
    unsigned long long total_size = buf.size();
    if((unsigned long long)off > total_size) {
        buf = buf.append(off-total_size, '\0');
        total_size = buf.size();
    }
    std::string to_write;
    to_write.assign(data, size);
    buf = buf.replace(off, size, to_write);
    ec->put(ino, buf);
    lc->release(ino);
    bytes_written = size;
    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    lc->acquire(parent);
    bool found = false;
    inum id;
    unsigned long long length;
    std::string buf;
    ec->get(parent, buf);
    const char *c_str = buf.c_str();
    unsigned long long left = buf.length();
    char *ptr = (char *)c_str;

    unsigned long long dir_new_length = left;
    char *dir_new = new char[left];
    char *ptr_new = dir_new;
    while(left > 0) {
        memcpy(&id, ptr, sizeof(inum));
        ptr += sizeof(inum);
        memcpy(&length, ptr, sizeof(unsigned long long));
        ptr += sizeof(unsigned long long);
        char *name_buf = new char[length + 1];
        memcpy(name_buf, ptr, length);
        ptr += length;
        name_buf[length] = '\0';
        if(strcmp(name_buf, name)) {
            memcpy(ptr_new, &id, sizeof(inum));
            ptr_new += sizeof(inum);
            memcpy(ptr_new, &length, sizeof(unsigned long long));
            ptr_new += sizeof(unsigned long long);
            memcpy(ptr_new, name_buf, length);
            ptr_new += length;
        }
        else {
            found = true;
            ec->remove(id);
        }
        left -= (sizeof(inum) + sizeof(unsigned long long) + length);
        delete []name_buf;
    }
    if(found) {
        dir_new_length -= (sizeof(inum) + sizeof(unsigned long long) + strlen(name));
        std::string buf_new;
        buf_new.assign(dir_new, dir_new_length);
        ec->put(parent, buf_new);
    }
    else {
        r = NOENT;
    }
    delete []dir_new;
    
    lc->release(parent);
    return r;
}

int yfs_client::readlink(inum ino, char *&link)
{
    int r = OK;
    std::string buf;
    lc->acquire(ino);
    if(ec->get(ino, buf) != extent_protocol::OK) {
        lc->release(ino);
        link = NULL;
        return NOENT;
    }
    lc->release(ino);
    char *link_str = new char[buf.length()];
    strncpy(link_str, buf.c_str(), buf.length() + 1);
    link = link_str;
    return r;
}

int yfs_client::symlink(inum parent, const char *link, const char *name, inum &ino_out)
{
    int r = OK;

    bool found = false;
    inum id;
    lc->acquire(parent);
    lookup_nolock(parent, name, found, id);
    if(found == true) {
        lc->release(id);
        return EXIST;
    }
    
    if(ec->create(extent_protocol::T_SLINK, id) != extent_protocol::OK) {
        lc->release(parent);
        printf("error when creating symbol link!\n");
        return IOERR;
    }
    
    ino_out = id;
    std::string buf;
    ec->get(parent, buf);
    unsigned long long length = strlen(name);
    unsigned long long tol_len = sizeof(inum) + sizeof(unsigned long long) + length;
    char *entry = new char[tol_len];
    memcpy(entry, &id, sizeof(inum));
    memcpy(entry + sizeof(inum), &length, sizeof(unsigned long long));
    memcpy(entry + sizeof(inum) + sizeof(unsigned long long), name, length);
    std::string buf_tmp;
    buf_tmp.assign(entry, tol_len);
    delete []entry;
    buf = buf.append(buf_tmp);
    ec->put(parent, buf);
    lc->acquire(id);
    lc->release(parent);

    ec->get(id, buf);
    buf.assign(link, strlen(link));
    ec->put(id, buf);
    lc->release(id);
    return r;
}
