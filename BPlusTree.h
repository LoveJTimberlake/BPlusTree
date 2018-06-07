#include<iostream>
using namespace std;

#define Min_Cache_Num 5		//5 cache for node or sibling

#define list_entry(ptr,type,member) \
	((type*)((char*)(ptr) - (size_t)(&((type*)0)->member)))
	//This is to return the address of this member in this struct

#define list_first_entry(ptr,type,member) \ 
	list_entry((ptr)->next,type,member)
	//This is to return the address of this member in next struct 

#define list_last_entry(ptr,type,member) \
	list_entry((ptr)->prev,type,member)
	//This is to return the address of this member in the last one struct

#define list_for_each(pos,head) \
	for(pos = (head)->next; pos != head; pos = pos->next)

#define list_for_each_safe(pos,n,head) \
	for(pos = (head)->next, n = pos->next; pos != head ; pos = n, n = pos->next)

typedef int key_t;

struct list_head{
	 list_head * next, * prev
};

static inline void list_init( list_head * link)	//Init the first node in list or make the deleted node's ptr safe(Double ptr all points to itself)
{
	link->prev = link;
	link->next = link;
}

static inline void __list_add( list_head * link, list_head * prev,list_head * next)		//Add a new node link between prev and next
{
	link->next = next;
	link->prev = prev;
	next->prev = link;
	prev->next = link;
}

static inline void __list_del(list_head * prev, list_head * next)		//Delete the node between prev and next
{
	prev->next = next;
	next->prev = prev;
}

static inline void list_add(list_head * link, list_head * prev)
{
	__list_add(link,prev,prev->next);
}

static inline void list_add_tail(list_head * link,list_head * head)	//Becaust it is a circle list, its tail is prev to head, so new tail should be between head and old tail
{
	__list_add(link,head->prev,head);
}

static inline void list_del(list_head * link)
{
	__list_del(link->prev,link->next);
	list_init(link);
}

static inline int list_empty(const list_head * head)	//It's a circle list so if it is empty, its head should be equal to its next
{
	return head->next == head;
}

typedef struct bplus_node 
{
	off_t self;
	off_t parent;
	off_t prev;
	off_t next;
	int type;
	int children;	//LeafNode specifies count of entries and NonLeafNode specifies count of its branches
}bplus_node;

typedef struct free_block
{
	struct list_head link;
	off_t offset;
}free_block;

struct bplus_tree
{
	char * caches;
	int used[Min_Cache_Num];
	char filename[1024];
	int fd;
	int level;
	off_t root;
	off_t file_size;
	struct list_head free_blocks;
};

void bplus_tree_dump( bplustree * tree);
long bplus_tree_get(bplus_tree * tree, key_t key);
int bplus_tree_put(bplus_tree * tree, key_t key);
long bplus_tree_get_range(bplus_tree * tree,key_t key1,key_t key2);
struct bplus_tree * bplus_tree_init(char * filename, int block_size);
void bplus_tree_deinit(bplus_tree * tree);
int bplus_open(char * filename);
void bplus_close(int fd);

