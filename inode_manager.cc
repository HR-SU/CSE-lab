#include "inode_manager.h"
#include <time.h>

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
<<<<<<< HEAD
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

=======
  if(buf == NULL || id >= BLOCK_NUM) return;
>>>>>>> lab1
  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
<<<<<<< HEAD
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

=======
  if(buf == NULL || id >= BLOCK_NUM) return;
>>>>>>> lab1
  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  char buf[BLOCK_SIZE];
  d->read_block(BBLOCK(sb.nblocks/BPB + INODE_NUM/IPB + 3), buf);
  for(uint32_t i = sb.nblocks/BPB + INODE_NUM/IPB + 3; i < sb.nblocks; i++) {
    if(BBLOCK(i) != BBLOCK(i-1))
      d->read_block(BBLOCK(i), buf);
    uint32_t index = (i % BPB) / 8;
    uint32_t offset = (i % BPB) % 8;
    unsigned char mask = (unsigned char)(0x80 >> offset);
    if((buf[index] & mask) == (char)0) {
      buf[index] |=  mask;
      d->write_block(BBLOCK(i), buf);
      return i;
    }
  }
  return 0;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  if(id >= BLOCK_NUM) return;
  char buf[BLOCK_SIZE];
  d->read_block(BBLOCK(id), buf);
  uint32_t index = (id % BPB) / 8;
  uint32_t offset = (id % BPB) % 8;
  unsigned char mask = ~(unsigned char)(0x80 >> offset);
  buf[index] &= mask;
  d->write_block(BBLOCK(id), buf);
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  for(uint32_t inum = 1; inum < INODE_NUM; inum++) {
    struct inode* ino = get_inode(inum);
    if(ino == NULL) {
      ino = (struct inode*)malloc(sizeof(struct inode));
      ino->type = type;
      ino->size = 0;
      ino->atime = time(NULL);
      ino->mtime = time(NULL);
      ino->ctime = time(NULL);
      memset(ino->blocks, 0, sizeof(ino->blocks));
      put_inode(inum, ino);
      free(ino);
      return inum;
    }
  }
  
  struct inode* ino = get_inode(1);
  ino->type = type;
  ino->size = 0;
  ino->atime = time(NULL);
  ino->mtime = time(NULL);
  ino->ctime = time(NULL);
  memset(ino->blocks, 0, sizeof(ino->blocks));
  put_inode(1, ino);
  free(ino);
  return 1;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  struct inode* ino = get_inode(inum);
  if(ino->type == 0) return;
  if(ino->size != 0) {
    printf("\tim: cannot free inode\n");
    return;
  }
  memset(ino, 0, sizeof(struct inode));
  put_inode(inum, ino);
  free(ino);
  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */
  struct inode* ino = get_inode(inum);
  uint32_t left = ino->size;
  uint32_t nblock = left / BLOCK_SIZE;
  if(left % BLOCK_SIZE) nblock++;
  char *buf_out_tmp = (char *)malloc(sizeof(char) * left);
  char *ptr = buf_out_tmp;
  char buf[BLOCK_SIZE];
  uint32_t block_index = 0;
  uint32_t nblock_direct = MIN(nblock, NDIRECT);
  for(; block_index < nblock_direct; block_index++) {
    bm->read_block(ino->blocks[block_index], buf);
    uint32_t to_copy = MIN(left, BLOCK_SIZE);
    memcpy(ptr, buf, to_copy);
    ptr += to_copy;
    left -= to_copy;
  }
  if(block_index == NDIRECT && left > 0) {
    char buf_indirect[BLOCK_SIZE];
    bm->read_block(ino->blocks[block_index], buf_indirect);
    uint32_t nblock_max = BLOCK_SIZE / sizeof(blockid_t) + NDIRECT;
    blockid_t block_id;
    char *bidptr = buf_indirect;
    while(block_index < nblock_max && left > 0) {
      memcpy(&block_id, bidptr, sizeof(blockid_t));
      bidptr += sizeof(blockid_t);
      bm->read_block(block_id, buf);
      uint32_t to_copy = MIN(left, BLOCK_SIZE);
      memcpy(ptr, buf, to_copy);
      ptr += to_copy;
      left -= to_copy;
      block_index++;
    }
  }
  *size = ino->size;
  *buf_out = buf_out_tmp;
  ino->atime = time(NULL);
  put_inode(inum, ino);
  free(ino);
  return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  struct inode* ino = get_inode(inum);
  uint32_t nblock_old = ino->size / BLOCK_SIZE;
  uint32_t nblock_new = size / BLOCK_SIZE;
  if(ino->size % BLOCK_SIZE) nblock_old++;
  if(size % BLOCK_SIZE) nblock_new++;
  uint32_t nblock_new_direct = MIN(nblock_new, NDIRECT);
  uint32_t nblock_old_direct = MIN(nblock_old, NDIRECT);
  uint32_t nblock_max = BLOCK_SIZE / sizeof(blockid_t) + NDIRECT;
  char buf_data[BLOCK_SIZE];
  uint32_t left = size;
  char *ptr = (char *)buf;
  uint32_t block_index = 0;
  if(nblock_new <= nblock_old) {
    for(; block_index < nblock_old_direct; block_index++) {
      if(block_index >= nblock_new) {
        bm->free_block(ino->blocks[block_index]);
        continue;
      }
      uint32_t to_copy = MIN(left, BLOCK_SIZE);
      blockid_t bid = ino->blocks[block_index];
      bm->read_block(bid, buf_data);
      memcpy(buf_data, ptr, to_copy);
      bm->write_block(bid, buf_data);
      left -= to_copy;
      ptr += to_copy;
    }
    if(block_index == NDIRECT && nblock_old > NDIRECT) {
      char buf_indirect[BLOCK_SIZE];
      blockid_t bid = ino->blocks[block_index];
      bm->read_block(bid, buf_indirect);
      char *bidptr = buf_indirect;
      uint32_t nblock_old_max = MIN(nblock_old, nblock_max);
      while(block_index < nblock_old_max) {
        memcpy(&bid, bidptr, sizeof(blockid_t));
        bidptr += sizeof(blockid_t);
        if(block_index >= nblock_new) {
          bm->free_block(bid);
          continue;
        }
        uint32_t to_copy = MIN(left, BLOCK_SIZE);
        bm->read_block(bid, buf_data);
        memcpy(buf_data, ptr, to_copy);
        bm->write_block(bid, buf_data);
        ptr += to_copy;
        left -= to_copy;
        block_index++;
      }
    }
    if(nblock_old > NDIRECT && nblock_new <= NDIRECT) {
      bm->free_block(ino->blocks[NDIRECT]);
    }
  }
  else {
    for(; block_index < nblock_new_direct; block_index++) {
      unsigned int to_copy = MIN(left, BLOCK_SIZE);
      blockid_t bid = ino->blocks[block_index];
      if(block_index >= nblock_old) {
        bid = bm->alloc_block();
        ino->blocks[block_index] = bid;
      }
      bm->read_block(bid, buf_data);
      memcpy(buf_data, ptr, to_copy);
      bm->write_block(bid, buf_data);
      left -= to_copy;
      ptr += to_copy;
    }
    if(nblock_old <= NDIRECT && nblock_new > NDIRECT) {
      ino->blocks[NDIRECT] = bm->alloc_block();
    }
    if(block_index == NDIRECT && left > 0) {
      char buf_indirect[BLOCK_SIZE];
      blockid_t bid = ino->blocks[block_index];
      bm->read_block(bid, buf_indirect);
      char *bidptr = buf_indirect;
      uint32_t nblock_max = BLOCK_SIZE / sizeof(blockid_t) + NDIRECT;
      while(block_index < nblock_max && left > 0) {
        uint32_t to_copy = MIN(left, BLOCK_SIZE);
        if(block_index >= nblock_old) {
          bid = bm->alloc_block();
          memcpy(bidptr, &bid, sizeof(blockid_t));
        }
        else memcpy(&bid, bidptr, sizeof(blockid_t));
        bidptr += sizeof(blockid_t);
        bm->read_block(bid, buf_data);
        memcpy(buf_data, ptr, to_copy);
        bm->write_block(bid, buf_data);
        ptr += to_copy;
        left -= to_copy;
        block_index++;
      }
      bm->write_block(ino->blocks[NDIRECT], buf_indirect);
    }
  }
  if(ino->size != (unsigned int)size) ino->ctime = time(NULL);
  ino->size = size;
  ino->atime = time(NULL);
  ino->mtime = time(NULL);
  put_inode(inum, ino);
  free(ino);
  return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  struct inode* ino = get_inode(inum);
  if(ino == NULL) {
    memset(&a, 0, sizeof(extent_protocol::attr));
    return;
  }
  a.type = ino->type;
  a.size = ino->size;
  a.atime = ino->atime;
  a.mtime = ino->mtime;
  a.ctime = ino->ctime;
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  struct inode* ino = get_inode(inum);
  if(ino->size != 0) {
    uint32_t nblocks = ino->size/BLOCK_SIZE + 1;
    for(uint32_t i = 0; i < nblocks; i++) {
      bm->free_block(ino->blocks[i]);
    }
  }
  memset(ino->blocks, 0, sizeof(ino->blocks));
  ino->size = 0;
  put_inode(inum, ino);
  free(ino);
  free_inode(inum);
  return;
}
