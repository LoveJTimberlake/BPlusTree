#include<iostream>
#include<stdio.h>
#include<stdlib.h>
#include<assert.h>
#include<string>
#include<fcntl.h>
#include<ctype.h>
#include<unistd.h>
#include<sys/types.h>
#include<sys/stat.h>
#include"BPlusTree.h"

enum { Invalid_Offset = 0xdeadbeef,};

enum { BPlus_Tree_Leaf, BPlus_Tree_NonLeaf = 1};

enum { Left_siblinng, Right_Sibling = 1};

#define Addr_Str_Width 16
#define offset_ptr(node) ((char*) (node) + sizeof(node))
#define key(node) ((key_t*)offset_ptr(node))
#define data(node) ((long*) (offset_ptr(node) + _max_entries * sizeof(key_t)))
#define sub(node) ((off_t *) (offset_ptr(node) + (_max_order -1) * sizeof(key_t)))

static int _block_size;
static int _max_entries;
static int _max_order;

static inline int is_leaf(bplus_node * node)
{
	return node->type == BPlus_Tree_Leaf;
}

static int Key_Binary_Search(bplus_node * node, key_t target)	//binary search the target value in leaf_node array
{
	key_t *arr = key(node);
	int len = is_leaf(node) ? node->children : node->children -1;
	int low = -1;
	int high = len;
	
	while(low + 1 < length)
	{
		int mid = low + (high-low)/2;
		if(target > arr[mid]) low = mid;
		else high = mid;
	} 
	if(high >= len || arr[high] != target)	return -high-1;
	else return high;
}

static inline int parent_key_index(bplus_node * parent, key_t key)	//return the index of target value
{
	int index = key_binary_search(parent,key);
	return index >= 0 ? index : -index - 2;
}

static inline bplus_node* cache_refer(bplus_tree * tree)	//Return a record about Tree's unused cache
{
	int i ;
	for(i = 0; i < Min_Cache_Num;i++)
	{
		if(!tree->used[i]) 
		{
			tree->used[i] = 1;
			char * buf = tree->caches + _block_size * i;
			return (bplus_tree*) buf;
		}
	}
	assert(0);
}

static inline void cache_defer( bplus_tree * tree, bplus_tree * node)	//From func called cache_refer, char * node = tree->caches + _block_size * i, so if we need to refree the used cache of the tree, need to get i and update it to 0
{
	char * buf = (char*) node;
	int i = (buf - tree->caches) / _block_size;
	tree->used[i] = 0;
}

static bplus_node * node_new (bplus_tree * tree)	//Get a new node of tree
{
	bpuls_node * node = cache_refer(tree);
	node->self = Invalid_Offset;
	node->parent = Invalid_Offset;
	node->prev = Invalid_Offset;
	node->next = Invalid_Offset;
	node->children = 0;
	return node;
}

static inline bplus_node * non_leaf_new(bplus_tree * tree)	//get a new node of tree and set it's type to non-leaf
{
	bplus_node * node = node_new(tree);
	node->type = Bplus_Tree_Non_Leaf;
	return node;
}

static inline bplus_node * leaf_new(bplus_tree * tree)	//get a new node of tree and set its type to leaf
{
	bplus_node * node = node_new(tree);
	node->type = BPlus_Tree_Leaf;
	return node;
}

static bplus_node * node_fetch(bplus_tree * tree, off_t offset)	//get the wanted context in tree and put it into a new node returned
{
	if(offset == Invalid_Offset)	return NULL;
	bplus_node * node = cache_refer(tree);
	int len = pread(tree->fd, node, _block_size, offset);	//read _block_size bytes from tree->fd at offset(from the start) into node, return the length of bytes readed
	assert(len == _block_size);
	return node;
}

static bplus_node * node_seek(bplus_tree * tree, off_t offset)	//doubt
{
	if(offser == Invalid_Offset)	 return NULL;
	int i ;
	for(i = 0;i < Min_Cache_Num; i++)
	{
		if(!tree->used[i])
		{
			char * buf = tree->caches + _block_size * i;
			int len = pread(tree->fd,buf,_block_size,offset);
			assert(len == _block_size);
			return(bplus_tree*) buf;
		}
	}
	assert(0);
}

static inline void node_flush(bplus_tree * tree, bplus_tree * node)	//put the context of node into the corresponding loction in tree and defer it to unused
{
	if(node)
	{
		int len = pwrite(tree->fd, node, _block_size, node->self);
		assert(len == _block_size);
		cache_defer(tree, node);
	}	
}

static off_t new_node_append(bplus_tree * tree, bplus_node * node)	//
{
	if(list_empty(&tree->free_blocks))	//free_blocks is head of linklist
	{
		node->self = tree->file_size;	//file_size means the offset of file ending now, so the new node's address(offset) should be file_size
		tree->file_size += _block_size;	//_block_size is the size of node's data
	}
	else
	{
		free_block * block;
		block = list_first_entry(&tree->free_blocks, free_block, link);	//get the position of link of the next ptr to the free_blocks(head)
		list_del(&block->link);
		node->self = block->offset;
		free(block);
	}
	return node->self;
}

static void node_delete(bplus_tree * tree, bplus_node * node, bplus_node * left, bplus_node * right)	//node_flush() doubts
{
	if(left)
	{
		if(right)
		{
			left->next = right->self;
			right->prev = left->self;
			node_flush(tree,right);
		}
		else left->next = Invalid_Offset;
		node_flush(tree,left);
	}
	else
	{
		if(right)
		{
			right->prev = Invalid_Offset;
			node->flush(tree,right);
		}
	}
	
	assert(node->self != Invalid_Offset);
	free_block * block = new free_block;
	assert(block);
	block->offset = node->self;
	list_add_tail(&block->link, &tree->free_blocks);
	cache_defer(tree,node);
}

static inline void sub_node_update(bplus_tree * tree, bplus_node * parent, int index, bplus_node * sub_node)	//connect it to its parent node and flush it
{
	assert(sub_node->self != Invalid_Offset);
	sub(parent)[index] = sub_node->self;
	sub_node->parent = parent->self;
	node_flush(tree,sub_node);
}

static long bplus_tree_search(bplus_tree * tree, key_t key)	//
{
	int ret = -1;
	bplus_node * node = node_seek(tree,tree->root);
	while(node)
	{
		int i = key_binary_search(node,key);
		if(is_leaf(node))
		{
			ret = i >= 0 ? data(node)[i] : -1;
			break;
		}
		else 
		{
			if(i >= 0) node = node_seek(tree,sub(node)[i+1]);
			else 
			{
				i = -i - 1;
				node = node_seek(tree,sub(node)[i]);
			}
		}
	}
	return ret;
}

static void left_node_add(bplus_tree * tree, bplus_node * node, bplus_node * right)
{
	new_node_append(tree,right);

	bplus_node * next = node_fetch(tree,node->next);
	if(next)
	{
		next->prev = right->self;
		right->next = next->self;
		node_flush(tree,next);
	}
	else 	right->next = Invalid_Offset;
	right->prev = node->self;
	node->next = right->self;
}

static void right_node_add(bplus_tree * tree, bolus_node * node, bplus_node * right)
{
	new_node_append(tree,right);
	bplus_node * next = node_fetch(tree,node->next);
	if(next)
	{
		next->prev = right->self;
		right->next = next->self;
		node_flush(tree,next);
	}
	else 
	{
		right->next = Invalid_Offset;
	}
	right->prev = node->self;
	node->next = right->self;
}

static key_t non_leaf_insert(bplus_tree * tree, bplus_node * node, bplus_node * l_ch, bplus_node * r_ch, key_t key);

static int parent_node_build(bplus_tree * tree, bplus_node * l_ch, bplus_node * r_ch, key_t key)
{
	if(l_ch -> parent == Invalid_Offset && r_ch->parent == Invalid_Offset)
	{
		bplus_node * parent = non_leaf_new(tree);
		key(parent)[0] = key;
		key(parent)[0] = l_ch->self;
		key(parent)[1] = r_ch->self;
		parent->children = 2;
		tree->root = new_node_append(tree,parent);
		l_ch->parent = parent->self;
		r_ch->parent = parent->self;
		tree->level++;
		node_flush(tree,l_ch);
		node_flush(tree,r_ch);
		node_flush(tree,parent);
		return 0;
	}
	else if(r_ch->parent == Invalid_Offset) return non_leaf_insert(tree,node_fetch(tree,l_ch->parent),l_ch,r_ch,key);
	else return non_leaf_insert(tree,node_fetch(tree,r_ch->parent),l_ch,r_ch,key);
}

static key_t non_leaf_split_left(bplus_tree * tree, bplus_node * node, bplus_node * left, bplus_node * l_ch, bplus_node * r_ch, key_t key, int insert)
{
	int i;
	key_t split_key;
	int split = (_max_order + 1)/2;
	left_node_add(tree,node,left);
	int pivot = insert;
	left->children = split;
	node->children = _max_order - split + 1;
	
	memmove(&key(left)[0],&key(node)[0],pivot * sizeof(key_t));
	memmove(&sub(left)[0],&sub(node)[0],pivot * sizeof(key_t));
	memmove(&key(left)[pivot + 1],&key(node)[pivot],(split - pivot - 1) * sizeof(key_t));
	memmove(&sub(left)[pivot + 1], &sub(node)[pivot], (split - pivot - 1) * sizeof(off_t);

	for(i = 0; i < left->children; i++)
	{
		if(i != pivot && i != pivot + 1)	sub_node_flush(tree,left,sub(left)[i]);
	}
	
	key(left)[pivot] = key;
	if(pivot == split - 1)	
	{
		sub_node_update(tree,left,pivot,l_ch);
		sub_node_update(tree,node,0,r_ch);
		split_key = key;
	}
	else 
	{
		sub_node_update(tree,left,pivot,l_ch);
		sub_node_update(tree,left,pivot + 1, r_ch);
		sub(node)[0] = sub(node)[split - 1];
		split_key = key(node)[split-2];
	}
	// sum = node->children = 1 + (node->children - 1)
	//right node left shift from key[split - 1] to key[children - 2]
	memmove(&key(node)[0], &key(node)[split-1], (node->children - 1) * sizeof(key_t));
	memmove(&sub(node)[1].&sub(node)[split],(node->children - 1) * sizeof(off_t);
	return split_key;
}

static key_t non_leaf_split_right1(bplus_tree * tree, bplus_node * node, bolus_node * right, bplus_node * l_ch, bplus_node * r_ch, key_t key, int insert)
{
	int i,
	int split = (_max_number + 1) / 2;
	right_node_add(tree,node,right);
	key_t split_key = key(node)[split-1];
	int pivot = 0;
	node->children = split;
	right->children = _max_order - split + 1;
	key(right)[0] = key;
	sub_node_update(tree,right,pivot,l_ch);
	sub_node_update(tree,right,pivot + 1, r_ch);
	memmove(&key(right)[pivot+1], &key(node)[split],(right->children - 2) * sizeof(key_t));
	memmove(&sub(right)[pivot+2],&sub(node)[split+1],(right->children - 2) * sizeof(off_t));
	for(i = pivot + 2; i < right->children;i++)	sub_node_flush(tree,right,sub(right)[i]);
	return split_key;
}

static key_t non_leaf_split_right2(bplus_tree * tree, bplus_node * node, bplus_node * right, bplus_node * l_ch, bplus_node * r_ch)
{
	int i;
	int split = (_max_order + 1)/2;
	right_node_add(tree,node,right);
	key_t split_key = key(node)[split];
	
	int pivot = insert - split - 1;
	node->children = split + 1;
	right->children = _max_order - split;
	memmove(&key(right)[0],&key(node)[split+1],pivot * sizeof(key_t));
	memmove(&sub(right)[0],&sub(node)[split+1],pivot * sizeof(off_t));
	for(i = 0; i < right->children;i++)
	{
		if(i != pivot && i != pivot + 1)	sub_node_flush(tree,right,sub(right)[i]);
	}
	return split_key;
}

static void non_leaf_simple_insert(bplus_tree * tree, bplus_node * node, bplus_node * l_ch, bplus_node * r_ch,key_t key, int insert)
{
	memmove(&key(node)[insert + 1],&key(node)[insert],(node->children - 1 - insert) * sizeof(key_t);
	memmove(&sub(node)[insert + 2], &sub(node)[insert+1],(node->children - 1 - insert) * sizeof(off_t));
	key(node)[insert]  = key;
	sub_node_update(tree,node,insert,l_ch);
	sub_node_update(tree,node,insert,r_ch);
	node->children ++;
}

static int non_leaf_insert(bplus_tree * tree, bplus_node * node, bplus_node * l_ch, bplus_node * r_ch.key_t key)
{
	int insert = key_binary_search(node,key);
	assert(insert < 0);
	insert = -insert - 1;
	if(node->children == _max_order)
	{
		key_t split_key;
		int split = (node->children + 1) / 2;
		bplus_node * sibling = non_leaf_new(tree);
		if(insert < split)	split_key = non_leaf_split_left(tree,node,sibling,l_ch,r_ch,key,insert);
		else if(insert == split)	split_key = non_leaf_split_right1(tree,node,sibling,l_ch,r_ch,key,insert);
		else split_key = non_leaf_split_right2(tree,node,sibling,l_ch,r_ch,key,insert);
		if(insert < split) return parent_node_build(tree,sibling,node,split_key);
		else return parent_node_build(tree,node,sibling,split_key);
	}
	else 
	{
		non_leaf_simple_start(tree,node,l_ch,r_ch,key,insert);
		node_flush(tree,node);
	}
	return 0;
}


static key_t leaf_split_left(bplus_tree * tree, bplus_node * leaf, bplus_node * left, key_t key, long data,int insert)
{
	int split = (leaf->children + 1) / 2;
	left_node_add(tree,leaf,left);
	int pivot = insert;
	left->children = split;
	leaf->children = _max_entries - split + 1;
	memmove(&key(left)[0],&key(leaf)[0],pivot * sizeof(key_t));
	memmove(&data(left)[0],&data(leaf)[0],pivot * sizeof(long));

	key(left)[pivot] = key;
	data(left)[pivot] = data;
	
	memmove(&key(left)[pivot + 1], &key(leaf)[pivot],(split - pivot - 1) * sizeof(key_t));
	memmove(&data(left)[pivot + 1], &data(leaf)[pivot],(split - pivot - 1) * sizeof(long));
	memmove(&key(leaf)[0],&key(leaf)[split - 1], leaf->children * sizeof(key_t));
	memmove(&data(leaf)[0],&data(leaf)[split - 1], leaf->children * sizeof(long));
	return key(leaf)[0];
}


















































