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

typedef unsigned long long cycles_t;

inline cycles_t currentcycles() {
    cycles_t result;
    __asm__ __volatile__ ("rdtsc": "=A"(result));
    return result;
}

yfs_client::yfs_client()
{
    ec = new extent_client();

}

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
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
    //cycles_t start = currentcycles();
    extent_protocol::attr a;
    
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return (extent_protocol::types)0;
    }

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
    //printf("yfs: filetype cost %lld cycles\n", currentcycles()-start);
    return (extent_protocol::types)a.type;
}
/*
bool
yfs_client::isfile(inum inum)
{
    cycles_t start = currentcycles();
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        printf("yfs: isfile cost %lld cycles\n", currentcycles()-start);
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        printf("yfs: isfile cost %lld cycles\n", currentcycles()-start);
        return true;
    } 
    printf("isfile: %lld is not a file\n", inum);
    printf("yfs: isfile cost %lld cycles\n", currentcycles()-start);
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
    cycles_t start = currentcycles();
    // Oops! is this still correct when you implement symlink?
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        printf("yfs: isdir cost %lld cycles\n", currentcycles()-start);
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isdir: %lld is a directory\n", inum);
        printf("yfs: isdir cost %lld cycles\n", currentcycles()-start);
        return true;
    } 
    printf("isdir: %lld is not a directory\n", inum);
    printf("yfs: isdir cost %lld cycles\n", currentcycles()-start);
    return false;
}

bool
yfs_client::issymlink(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

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
    cycles_t start = currentcycles();
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("yfs_client: getdir: IOERR\n");
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    printf("yfs: getfile cost %lld cycles\n", currentcycles()-start);
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    cycles_t start = currentcycles();
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("yfs_client: getdir: IOERR\n");
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    printf("yfs: getdir cost %lld cycles\n", currentcycles()-start);
    return r;
}

int
yfs_client::getsymlink(inum inum, symlinkinfo &slin)
{
    int r = OK;

    std::string buf;
    printf("getsymlink %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("yfs_client: getdir: IOERR\n");
        r = IOERR;
        goto release;
    }
    
    if(ec->get(inum, buf) != extent_protocol::OK) {
        r = NOENT;
        goto release;
    }

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
    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    //cycles_t start = currentcycles();
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    inum id;
    if(ec->create(extent_protocol::T_FILE, id) != extent_protocol::OK) {
        printf("error when creating file!\n");
    }
    //cycles_t check1 = currentcycles();
    ino_out = id;
    std::string buf;
    ec->get(parent, buf);
    //cycles_t check2 = currentcycles();
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
    //cycles_t check3 = currentcycles();
    ec->put(parent, buf);
    //printf("yfs: create %lld, %lld, %lld, %lld\n", check1-start, check2-check1, check3-check2, currentcycles()-check3);
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
    lookup(parent, name, found, ino_out);
    if(found == true) return EXIST;

    inum id;
    if(ec->create(extent_protocol::T_DIR, id) != extent_protocol::OK) {
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
    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    ///cycles_t start = currentcycles();
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    found = false;
    std::string buf;
    ec->get(parent, buf);
    //cycles_t check1 = currentcycles();
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
    //printf("yfs: lookup %lld, %lld\n", check1-start, currentcycles()-check1);
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
    ec->get(ino, buf);
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
    //cycles_t start = currentcycles();
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    std::string buf;
    ec->get(ino, buf);
    //cycles_t check1 = currentcycles();
    unsigned long long total_size = buf.size();
    if((unsigned long long)off > total_size) {
        buf = buf.append(off-total_size, '\0');
        total_size = buf.size();
    }
    std::string to_write;
    to_write.assign(data, size);
    buf = buf.replace(off, size, to_write);
    //cycles_t check2 = currentcycles();
    ec->put(ino, buf);
    bytes_written = size;
    //printf("yfs: write %lld, %lld, %lld\n", check1-start, check2-check1, currentcycles()-check2);
    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    //cycles_t start = currentcycles();
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
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
    //printf("yfs: unlink cost %lld cycles\n", currentcycles()-start);
    return r;
}

int yfs_client::readlink(inum ino, char *&link)
{
    int r = OK;
    std::string buf;
    if(ec->get(ino, buf) != extent_protocol::OK) {
        link = NULL;
        return NOENT;
    }
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
    lookup(parent, name, found, id);
    if(found == true) return EXIST;

    if(ec->create(extent_protocol::T_SLINK, id) != extent_protocol::OK) {
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

    ec->get(id, buf);
    buf.assign(link, strlen(link));
    ec->put(id, buf);
    return r;
}
