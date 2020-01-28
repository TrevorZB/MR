#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include "mapreduce.h"

struct node {
  char *key;
  char *value;
  struct node *next;
};

struct node_hub {
  struct node *head;
};

struct node_hub *parts;
Partitioner partitioner;
Getter getter;
Mapper mapper;
Reducer reducer;
int number_partitions;
sem_t *mutex;
struct node_hub *positions;

struct node *find_middle(struct node* start) {
  // return null if start is null
  if (start == NULL) {
    return NULL;
  }
  // set node pointers to the node passed in
  struct node *skip_one, *skip_two;
  skip_one = skip_two = start;
  // while the next next is not null, skip_one goes forward once
  // and skip_two goes forward twice
  while (skip_two->next != NULL && skip_two->next->next != NULL) {
    skip_one = skip_one->next;
    skip_two = skip_two->next->next;
  }
  // skip_one will be in the center, return it
  return skip_one;
}

struct node *merge(struct node *left, struct node *right) {
  // used to start the new constructed list
  struct node combo;
  // save tail
  struct node *tail = &combo;
  // temp used to save previous position
  struct node *temp;
  // iterate over each of the passed in lists
  while (left != NULL && right != NULL) {
    // left comes before alphabetically, or they are equal
    if (strcmp(left->key, right->key) <= 0) {
      // save left's next position
      temp = left->next;
      // add left to end of list
      left->next = NULL;
      tail->next = left;
      // move tail to new tail
      tail = tail->next;
      // move left along one
      left = temp;
    } else {
      // save right's next position
      temp = right->next;
      // add right to end of list
      right->next = NULL;
      tail->next = right;
      // move tail to new tail
      tail = tail->next;
      // move left along one
      right = temp;
    }
  }
  // right is used up, but left has nodes left
  if (left != NULL) {
    // add the rest of left to the end of the list
    tail->next = left;
  } else if (right != NULL) {
    // left is used up, but right has nodes left
    // add the rest of right to the end of the list
    tail->next = right;
  }
  // return the node after the placeholder combo
  return combo.next;
}

struct node *merge_sort(struct node *head) {
  // base case
  if (head == NULL || head->next == NULL) {
    return head;
  }
  // find middle
  struct node *middle = find_middle(head);
  // left is just head
  struct node *left = head;
  // right is one after middle
  struct node *right = middle->next;
  // cut link between the two sub lists
  middle->next = NULL;
  // recursively merge the lists
  return merge(merge_sort(left), merge_sort(right));
}

char *get_next(char *key, int partition_number) {
  // set temp to the position
  struct node *temp = positions[partition_number].head;
  // temp is null, end of list reached, return null
  if (temp == NULL) {
    return NULL;
  }
  // keys match, move position to next position
  if (strcmp(temp->key, key) == 0) {
    positions[partition_number].head = positions[partition_number].head->next;
    return temp->value;
  }
  // keys don't match, return null
  return NULL;
}

void free_nodes(struct node *head) {
  if (head == NULL) {
    return;
  }
  free_nodes(head->next);
  free(head->key);
  free(head->value);
  free(head);
}

void *Map_Wrap(void *filename) {
  mapper((char *)filename);
  return NULL;
}

void *Reduce_Wrap(void *arg) {
  int i = *(int *)arg;
  free(arg);
  // continues to iterate until temp is at end of the list
  while (1) {
    // set temp to position
    struct node *temp = positions[i].head;
    // if temp is null, no more keys to reduce
    if (temp == NULL) {
      break;
    }
    // there is a key to reduce, call reduce
    reducer(temp->key, getter, i);
  }
  return NULL;
}

void *Sort_Wrap(void *arg) {
  int i = *(int *)arg;
  free(arg);
  // sort the designated partition
  parts[i].head = merge_sort(parts[i].head);
  return NULL;
}

void *Free_Wrap(void *arg) {
  struct node *temp = (struct node *)arg;
  free_nodes(temp);
  return NULL;
}

void MR_Run(int argc, char *argv[],
  Mapper map, int num_mappers,
  Reducer reduce, int num_reducers,
  Partitioner partition, int num_partitions) {
  int i, j, z, *k;
  // initialize partition array and mutex array
  parts = malloc(sizeof(struct node_hub) * num_partitions);
  mutex = malloc(sizeof(sem_t) * num_partitions);
  for (i = 0; i < num_partitions; i++) {
    parts[i].head = NULL;
    sem_init(&mutex[i], 0, 1);
  }

  // initialize passed in partition function
  partitioner = partition;

  // initialize the getter
  getter = get_next;

  // initialize the mapper
  mapper = map;

  // initialize the reducer
  reducer = reduce;

  // initialize number of partitions
  number_partitions = num_partitions;

  // variables to make linter happy
  int kNumMappers = num_mappers;
  int kNumReducers = num_reducers;
  int kNumPartitions = num_partitions;

  // array of mappers
  pthread_t mappers[kNumMappers];

  // array of reducers
  pthread_t reducers[kNumReducers];

  // array of freers
  pthread_t freers[kNumPartitions];

  // array of sorters
  pthread_t sorters[kNumPartitions];

  // mappers, loop until all files are mapped
  j = 1;
  while (1) {
    // assign mappers to files
    for (i = 0; i < num_mappers && j < argc; i++, j++) {
      pthread_create(&mappers[i], NULL, Map_Wrap, argv[j]);
    }
    // wait for the mappers to finsh
    for (z = 0; z < i; z++) {
      pthread_join(mappers[z], NULL);
    }
    // done with all partitions, break
    if (j >= argc) {
      break;
    }
  }
  // sort threads
  for (i = 0; i < num_partitions; i++) {
    k = malloc(sizeof(int));
    *k = i;
    pthread_create(&sorters[i], NULL, Sort_Wrap, k);
  }
  // wait for all the sorters to finish
  for (i = 0; i < num_partitions; i++) {
    pthread_join(sorters[i], NULL);
  }

  // sorting is done, initialize positions array to speed up get_next
  // also use for loop to destroy the semaphores
  positions = malloc(sizeof(struct node_hub) * num_partitions);
  for (i = 0; i < num_partitions; i++) {
    positions[i].head = parts[i].head;
    sem_destroy(&mutex[i]);
  }
  // after destroyed, free the mutex memory
  free(mutex);

  // reducers, keep looping until all partitions finished
  j = 0;
  while (1) {
    // assign reducers to partitions
    for (i = 0; i < num_reducers && j < num_partitions; i++, j++) {
      k = malloc(sizeof(int));
      *k = j;
      pthread_create(&reducers[i], NULL, Reduce_Wrap, k);
    }
    // wait for the reducers to finsh
    for (z = 0; z < i; z++) {
      pthread_join(reducers[z], NULL);
    }
    // done with all partitions, break
    if (j >= num_partitions) {
      break;
    }
  }
  // free positions array
  free(positions);
  // free the partition array
  for (i = 0; i < num_partitions; i++) {
    pthread_create(&freers[i], NULL, Free_Wrap, parts[i].head);
  }
  // wait for freers to finish
  for (i = 0; i < num_partitions; i++) {
    pthread_join(freers[i], NULL);
  }
  // free parts array
  free(parts);
}

void MR_Emit(char *key, char *value) {
  // get partition being used
  int partition = partitioner(key, number_partitions);

  // initialize the node and its data
  struct node *temp = malloc(sizeof(struct node));
  int key_size = strlen(key) + 1;
  int value_size = strlen(value) + 1;
  temp->key = malloc(key_size);
  temp->value = malloc(value_size);
  snprintf(temp->key, key_size, "%s%s", key, "\0");
  snprintf(temp->value, value_size, "%s%s", value, "\0");

  // unsorted insertion, put inside of locks
  sem_wait(&mutex[partition]);
  temp->next = parts[partition].head;
  parts[partition].head = temp;
  sem_post(&mutex[partition]);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
  unsigned long hash = 5381;
  int c;
  while ((c = *key++) != '\0')
    hash = hash * 33 + c;
  return hash % num_partitions;
}

unsigned long power(int x, int y) {
  int result = 1;

  for (int i = 0; i < y; i++) {
    result *= x;
  }
  return result;
}

unsigned long base2Log(int num_partitions) {
  unsigned long result;
  result = (num_partitions > 1) ? 1 + base2Log(num_partitions / 2) : 0;
  return result;
}

unsigned long MR_SortedPartition(char *key, int num_partitions) {
  int bit_number[32];
  int i, pow;
  char *end;
  unsigned long result;
  // grab the key as a long
  unsigned long number = strtoul(key, &end, 10);
  // find how many high bits are needed for the number of partitions
  int num_high_bits = base2Log(num_partitions);
  // store the bits of the number into the bit array
  for (i = 31; i >= 0; i--) {
    bit_number[i] = number & 1;
    number = number >> 1;
  }
  result = 0;
  pow = 0;
  // convert the high bits back into a long
  for (i = num_high_bits - 1; i >= 0; i--, pow++) {
    result += bit_number[i] * power(2, pow);
  }
  return result;
}
