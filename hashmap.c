#include <stdlib.h>
#include "hashmap.h"
#include <pthread.h>


struct hash_map* hash_map_new(size_t (*hash)(void*), int (*cmp)(void*,void*),
    void (*key_destruct)(void*), void (*value_destruct)(void*)) {
	if(hash == NULL || cmp == NULL || key_destruct==NULL || value_destruct == NULL){
    return NULL;
  }

  struct hash_map* map = malloc(sizeof(struct hash_map));
  map->hash = hash;
  map->cmp = cmp;
  map->key_destruct = key_destruct;
  map->value_destruct = value_destruct;
  map->size = 27;//PRIME
  map->used = 0;
  map->bigLock = malloc(sizeof(pthread_mutex_t));
  pthread_mutex_init(map->bigLock, NULL);
  map->entries = malloc(sizeof(struct entry*) * map->size);
  map->ent_lock = malloc(sizeof(pthread_mutex_t*) * map->size);
  int i =0;
  while(i<map->size){
    map->ent_lock[i] = malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(map->ent_lock[i], NULL);
    map->entries[i] = NULL;
    i++;
  }
	return map;
}

void putBack(struct hash_map* map, void* k, void* v) {

  //SAME AS PUT BUT IGNORES THE LOCK, ALSO NOTE: NOTHING ADDED TO USED
  if(map == NULL || k == NULL || v == NULL){
    return;
  }

  size_t hashed = (*map->hash)(k);
  int index = hashed % map->size;

  //HEAD
  if(map->entries[index] == NULL){
    map->entries[index] = malloc(sizeof(struct entry));
    map->entries[index]->key = k;
    map->entries[index]->value = v;
    map->entries[index]->next = NULL;
    map->entries[index]->tail = map->entries[index];
  }
  //NOT HEAD
  else{

    struct entry* current = map->entries[index]->tail;

    current->next = malloc(sizeof(struct entry));
    current->next->key = k;
    current->next->value = v;
    current->next->next = NULL;
    map->entries[index]->tail = current->next;
  }

}

void resize(struct hash_map* map){

  int bound = map->size;
  map->size = (map->size*2)+7;//KEEP IT ODD
  map->entries = realloc(map->entries, sizeof(struct entry*) * map->size);
  map->ent_lock = realloc(map->ent_lock, sizeof(pthread_mutex_t*) * map->size);
  struct entry* copy = NULL;//TO RE INSERT
  int i = 0;
  while(i<map->size){
    if(i<bound){
      struct entry* curr = map->entries[i];
      if(curr != NULL){
        if(copy == NULL){
          copy = map->entries[i];
        }
        else{
          copy->tail->next = map->entries[i];
          copy->tail = map->entries[i]->tail;
        }
      }
    }
    else{
      map->ent_lock[i] = malloc(sizeof(pthread_mutex_t));
      pthread_mutex_init(map->ent_lock[i], NULL);
    }
    pthread_mutex_lock(map->ent_lock[i]);
    map->entries[i] = NULL; //CLEAR
    pthread_mutex_unlock(map->ent_lock[i]);//LOCK AND UNLOCK TO LET CURRENT LOCKED THREAD FINISH
    i++;
  }

  //REINSERT
  while(copy!=NULL){
    struct entry* temp = copy->next;
    putBack(map, copy->key, copy->value);
    free(copy);
    copy=temp;
  }
}

void clearMap(struct hash_map* map){
  int i = 0;
  while(i<map->size){
    pthread_mutex_destroy(map->ent_lock[i]);
    free(map->ent_lock[i]);
    while(map->entries[i] != NULL){
      struct entry* curr = map->entries[i];
      map->entries[i] = curr->next;
      (*map->key_destruct)(curr->key);
      (*map->value_destruct)(curr->value);
      free(curr);
    }
    i++;
  }
}

void hash_map_put_entry_move(struct hash_map* map, void* k, void* v) {
  if(map == NULL || k == NULL || v == NULL){
    return;
  }

  hash_map_remove_entry(map, k);

  pthread_mutex_lock(map->bigLock);//MAKE SURE NOT BEING RESIZED
  if(map->used >= map->size*1.5){
    resize(map);
  }

  size_t hashed = (*map->hash)(k);
  int index = hashed % map->size;
  map->used++;
  pthread_mutex_lock(map->ent_lock[index]);//LOCK BUCKET
  pthread_mutex_unlock(map->bigLock);




  if(map->entries[index] == NULL){
    map->entries[index] = malloc(sizeof(struct entry));
    map->entries[index]->key = k;
    map->entries[index]->value = v;
    map->entries[index]->next = NULL;
    map->entries[index]->tail = map->entries[index];
  }
  else{

    struct entry* current = map->entries[index]->tail;

    current->next = malloc(sizeof(struct entry));
    current->next->key = k;
    current->next->value = v;
    current->next->next = NULL;
    map->entries[index]->tail = current->next;
  }
  pthread_mutex_unlock(map->ent_lock[index]);
}

void hash_map_remove_entry(struct hash_map* map, void* k) {

  if(map == NULL || k == NULL){
    return;
  }

  pthread_mutex_lock(map->bigLock);//MAKE SURE NOT BEING RESIZED
  size_t hashed = (*map->hash)(k);
  int index = hashed % map->size;
  pthread_mutex_lock(map->ent_lock[index]);//LOCK BUCKET
  pthread_mutex_unlock(map->bigLock);

  struct entry* current = map->entries[index];
  if(current == NULL){
    pthread_mutex_unlock(map->ent_lock[index]);
    return;
  }


  struct entry* prev = NULL;
  while(current!=NULL){
    if((*map->cmp)(current->key, k) == 1){
      //NOT HEAD
      if(prev != NULL){
        prev->next = current->next;
        if(prev->next == NULL){
          map->entries[index]->tail = prev;
        }
      }
      //HEAD
      else{
        map->entries[index] = current->next;
        if(map->entries[index]!=NULL){
          map->entries[index]->tail = current->tail;
        }
      }

      //FREE ENTRY
      (*map->key_destruct)(current->key);
      (*map->value_destruct)(current->value);
      free(current);
      map->used--;
      pthread_mutex_unlock(map->ent_lock[index]);
      return;
    }
    prev = current;
    current = current->next;
  }
  pthread_mutex_unlock(map->ent_lock[index]);
}

void* hash_map_get_value_ref(struct hash_map* map, void* k) {

  if(map == NULL || k == NULL){
    return NULL;
  }

  pthread_mutex_lock(map->bigLock);//MAKE SURE NOT RESIZING
  size_t hashed = (*map->hash)(k);
  int index = hashed % map->size;
  pthread_mutex_lock(map->ent_lock[index]);//LOCK BUCKET
  pthread_mutex_unlock(map->bigLock);

  struct entry* current = map->entries[index];
  if(current == NULL){
    pthread_mutex_unlock(map->ent_lock[index]);
    return NULL;
  }

  while(current!=NULL){
    //FOUND
    if((*map->cmp)(current->key, k) == 1){
      pthread_mutex_unlock(map->ent_lock[index]);
      return current->value;
    }
    current = current->next;
  }
  //NOT FOUND
  pthread_mutex_unlock(map->ent_lock[index]);
  return NULL;
}

void hash_map_destroy(struct hash_map* map) {
  clearMap(map);
  pthread_mutex_destroy(map->bigLock);
  free(map->bigLock);
  free(map->entries);
  free(map->ent_lock);
  free(map);
}
