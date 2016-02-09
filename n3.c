/*

MIT/X11 License
Copyright (c) 2016 Sean Pringle <sean.pringle@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
OR IMPLIED, ADLLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

*/

#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <stdarg.h>
#include <signal.h>
#include <ctype.h>
#include <string.h>
#include <unistd.h>
#include <regex.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <pthread.h>

#define PRIME_1000 997
#define PRIME_10000 9973
#define PRIME_100000 99991
#define PRIME_1000000 999983

void
abort ()
{
  raise(SIGABRT);
  exit(EXIT_FAILURE);
}

#define ensure(x) for ( ; !(x) ; abort() )

void
errortime ()
{
  time_t t = time(NULL);
  struct tm *ltime = localtime(&t);
  char buf[32];
  size_t len = strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S ", ltime);
  fwrite(buf, 1, len, stderr);
}

//#define errorf(...) do { fprintf(stderr, __VA_ARGS__); fputc('\n', stderr); } while(0)
#define errorf(...) do { errortime(); fprintf(stderr, __VA_ARGS__); fputc('\n', stderr); } while(0)

typedef int (*callback)(void*);

#define LINE 1024
#define PATH 256
#define ALIAS 128
#define POOL 100000
#define POOL_CACHE 300000

#define E_OK 0
#define E_PARSE 1
#define E_SERVER 2
#define E_MISSING 3
#define E_MEMORY 4

#define O_INSERT 1
#define O_DELETE 2
#define O_SELECT 3

typedef int (*delimiter)(int);
typedef uint64_t number_t;
typedef uint64_t counter_t;
typedef uint8_t byte_t;

typedef struct _pool_t {
  size_t size;
  size_t width;
  off_t first;
  FILE *head;
  FILE *data;
  byte_t *cache;
  size_t cached;
  pthread_mutex_t mutex;
} pool_t;

typedef struct _pool_persist_t {
  size_t size;
  size_t width;
  off_t first;
} pool_persist_t;

typedef struct _pair_t {
  off_t offset;
  off_t sibling;
  number_t key;
  number_t val;
} pair_t;

typedef struct _record_t {
  number_t id;
  off_t pairs;
  struct _record_t *next, *link;
} record_t;

typedef struct {
  size_t width;
  record_t **chains;
  record_t *least;
  record_t *most;
} store_t;

typedef struct _alias_t {
  char *str;
  number_t num;
  struct _alias_t *next;
} alias_t;

typedef struct {
  size_t width;
  alias_t **chains;
} dict_t;

typedef struct {
  size_t mem_used;
  size_t mem_limit;
  counter_t mem_limit_hit;
  char sock_path[LINE];
  char data_path[LINE];
  size_t max_packet;
  size_t max_threads;
} state_t;

typedef struct _self_t {
  int response;
} self_t;

struct _field_t;
struct _field_key_t;
struct _query_t;

typedef int (*field_cb)(struct _query_t*, struct _field_t*);
typedef int (*field_process_cb)(struct _query_t*, struct _field_t*, struct _field_key_t*, record_t *record, pair_t *pair);

typedef int (*query_callback)(struct _query_t*);

typedef struct _field_key_t {
  number_t key;
  char alias[ALIAS];
  struct _field_key_t *next;
} field_key_t;

typedef struct _field_t {
  field_key_t *fkeys;
  number_t val;
  number_t sum;
  number_t min;
  number_t max;
  number_t diff;
  field_cb prepare;
  field_process_cb process;
  field_cb cleanup;
  char alias[ALIAS];
  int count;
  int null;
  struct _field_t *next;
} field_t;

typedef struct _query_t {
  int have_range;
  int explicit_step;
  field_t *fields;
  number_t id;
  number_t low;
  number_t high;
  number_t step;
  int count;
  int filled;
  int all_fields;
  query_callback handler;
} query_t;

pthread_rwlock_t rwlock;
pthread_mutex_t state_mutex;
pthread_mutex_t alias_mutex;
pthread_mutex_t activity_mutex;
static pthread_key_t keyself;
#define self ((self_t*)pthread_getspecific(keyself))

FILE *activity;
store_t store;
state_t state;
dict_t dict;
int multithreaded;

void
mutex_lock (pthread_mutex_t *mutex)
{
  if (multithreaded)
    pthread_mutex_lock(mutex);
}

void
mutex_unlock (pthread_mutex_t *mutex)
{
  if (multithreaded)
    pthread_mutex_unlock(mutex);
}

void
rwlock_rdlock (pthread_rwlock_t *rwlock)
{
  if (multithreaded)
    pthread_rwlock_rdlock(rwlock);
}

void
rwlock_wrlock (pthread_rwlock_t *rwlock)
{
  if (multithreaded)
    pthread_rwlock_wrlock(rwlock);
}

void
rwlock_unlock (pthread_rwlock_t *rwlock)
{
  if (multithreaded)
    pthread_rwlock_unlock(rwlock);
}

pool_t pool_pair;

#define RE_NAME "[[:alpha:]][[:digit:][:alpha:]_.@-]*"

#define RE_NUMBER "([[:digit:]]+|" RE_NAME ")"

#define RE_LIST "[,[:alnum:]_.@-]+"

regex_t re_range;
#define RE_RANGE "^" RE_NUMBER ":" RE_NUMBER

regex_t re_where;
#define RE_WHERE "^" RE_NUMBER "[=<>!&|^]" RE_NUMBER "[[:space:]]"

regex_t re_field;
#define RE_FIELD "^" RE_NUMBER "[[:space:]]"

regex_t re_field_aggr;
#define RE_FIELD_AGGR "^(max|min|sum|first|last|mean|median|diff)[(]" RE_LIST "[)]" "[[:space:]]"

regex_t re_field_as;
#define RE_FIELD_AS "^as " RE_NAME

regex_t re_alias_set;
#define RE_ALIAS_SET "^[[:digit:]]+[[:space:]]+[[:alpha:]][[:digit:][:alpha:]_.@-]*$"

regex_t re_alias_get;
#define RE_ALIAS_GET "^" RE_NAME

void*
allocate (size_t bytes)
{
  mutex_lock(&state_mutex);

  if (state.mem_used + bytes >= state.mem_limit)
  {
    if (!state.mem_limit_hit)
      errorf("hit state.mem_limit %lu", state.mem_limit);
    state.mem_limit_hit++;
    mutex_unlock(&state_mutex);
    return NULL;
  }

  state.mem_used += bytes;
  mutex_unlock(&state_mutex);
  return malloc(bytes);
}

void
release (void *ptr, size_t bytes)
{
  if (ptr)
  {
    mutex_lock(&state_mutex);

    ensure(state.mem_used >= bytes)
      errorf("bad state.mem_used");

    state.mem_used -= bytes;
    mutex_unlock(&state_mutex);
    free(ptr);
  }
}

void
pool_flush (pool_t *pool)
{
  pool_persist_t tmp;

  tmp.size  = pool->size;
  tmp.width = pool->width;
  tmp.first = pool->first;

  ensure(fseeko(pool->head, 0, SEEK_SET) == 0)
    errorf("cannot seek %06lu.head", pool->size);

  ensure(fwrite(&tmp, 1, sizeof(pool_persist_t), pool->head) == sizeof(pool_persist_t))
    errorf("cannot write %06lu.head", pool->size);
}

void
pool_extend (pool_t *pool)
{
  errorf("extending pool: %06lu", pool->size);

  int bytes = pool->size * POOL;
  unsigned char *chunk = allocate(bytes);
  memset(chunk, 0, bytes);

  unsigned char *p = chunk;

  for (int i = 0; i < POOL; i++)
  {
    *((off_t*)p) = pool->width + i + 1;
    p += pool->size;
  }

  p -= pool->size;
  *((off_t*)p) = 0;

  pool->first = pool->width;

  ensure(fseeko(pool->data, pool->width * pool->size, SEEK_SET) == 0)
    errorf("cannot seek pool: %06lu.data", pool->size);

  ensure(fwrite(chunk, 1, bytes, pool->data) == bytes)
    errorf("cannot extend pool: %06lu", pool->size);

  pool->width += POOL;
  release(chunk, bytes);
}

void
pool_init (size_t size)
{
  char scratch[100];

  pool_t *pool = allocate(sizeof(pool_t));

  pool->size  = size;
  pool->width = 0;
  pool->first = 0;
  pool->head  = NULL;
  pool->data  = NULL;

  errorf("initializing pool: %06lu", pool->size);

  snprintf(scratch, sizeof(scratch), "%06lu.head", pool->size);

  ensure((pool->head = fopen(scratch, "w")))
    errorf("cannot create pool: %06lu.head", pool->size);

  snprintf(scratch, sizeof(scratch), "%06lu.data", pool->size);

  ensure((pool->data = fopen(scratch, "w")))
    errorf("cannot create pool: %06lu.data", pool->size);

  while (pool->width * pool->size < POOL_CACHE * pool->size)
    pool_extend(pool);

  pool->first = 1;

  pool_flush(pool);

  fclose(pool->head);
  fclose(pool->data);

  release(pool, sizeof(pool_t));
}

void
pool_flush_cache (pool_t *pool)
{
  off_t offset = pool->width * pool->size - pool->cached;

  ensure(fseeko(pool->data, offset, SEEK_SET) == 0)
    errorf("cannot seek pool: %06lu.data (offset %lu)", pool->size, offset);

  ensure(fwrite(pool->cache, 1, pool->cached, pool->data) == pool->cached)
    errorf("cannot read pool: %06lu.data", pool->size);
}

void
pool_prime_cache (pool_t *pool)
{
  off_t offset = pool->width * pool->size - pool->cached;

  ensure(fseeko(pool->data, offset, SEEK_SET) == 0)
    errorf("cannot seek pool: %06lu.data (offset %lu)", pool->size, offset);

  ensure(fread(pool->cache, 1, pool->cached, pool->data) == pool->cached)
    errorf("cannot read pool: %06lu.data", pool->size);
}

int
pool_open (pool_t *pool, size_t size)
{
  char scratch[100];

  memset(pool, 0, sizeof(pool_t));
  pool->size = size;

  snprintf(scratch, sizeof(scratch), "%06lu.head", pool->size);

  if (!(pool->head = fopen(scratch, "r+")))
  {
    errorf("cannot open pool: %06lu.head", pool->size);
    return 0;
  }

  snprintf(scratch, sizeof(scratch), "%06lu.data", pool->size);

  if (!(pool->data = fopen(scratch, "r+")))
  {
    errorf("cannot open pool: %06lu.data", pool->size);
    return 0;
  }

  pool_persist_t tmp;

  ensure(fread(&tmp, 1, sizeof(pool_persist_t), pool->head) == sizeof(pool_persist_t))
    errorf("cannot read pool: %06lu.head", pool->size);

  pool->width = tmp.width;
  pool->first = tmp.first;
  pool->cached = POOL_CACHE * pool->size;

  ensure((pool->cache = allocate(pool->cached)) && pool->cache)
  {
    errorf("cannot allocate pool memory: %06lu (%lu bytes)", pool->size, pool->cached);
    return 0;
  }

  pool_prime_cache(pool);

  return 1;
}

byte_t*
pool_offset_cached (pool_t *pool, off_t item)
{
  off_t item_offset  = item * pool->size;
  off_t cache_offset = pool->width * pool->size - pool->cached;
  return (item_offset > cache_offset) ? pool->cache + (item_offset - cache_offset): NULL;
}

void
pool_read_raw (pool_t *pool, off_t item, void *ptr)
{
  ensure(item < pool->width)
    errorf("attempt to access item %lu outside pool: %06lu", item, pool->size);

  ensure(item > 0)
    errorf("attempt to access item 0 in pool: %06lu", pool->size);

  byte_t *cached = pool_offset_cached(pool, item);

  if (cached)
  {
    memmove(ptr, cached, pool->size);
    return;
  }

  ensure(fseeko(pool->data, item * pool->size, SEEK_SET) == 0)
    errorf("cannot seek pool: %06lu.data", pool->size);

  ensure(fread(ptr, 1, pool->size, pool->data) == pool->size)
    errorf("cannot read pool: %06lu.head, ferror: %d, feof: %d", pool->size, ferror(pool->data), feof(pool->data));
}

void
pool_read (pool_t *pool, off_t item, void *ptr)
{
  mutex_lock(&pool->mutex);
  pool_read_raw(pool, item, ptr);
  mutex_unlock(&pool->mutex);
}

void
pool_write_raw (pool_t *pool, off_t item, void *ptr)
{
  ensure(item < pool->width)
    errorf("attempt to access item %lu outside pool: %06lu", item, pool->size);

  ensure(item > 0)
    errorf("attempt to access item 0 in pool: %06lu", pool->size);

  byte_t *cached = pool_offset_cached(pool, item);

  if (cached)
  {
    memmove(cached, ptr, pool->size);
    return;
  }

  ensure(fseeko(pool->data, item * pool->size, SEEK_SET) == 0)
    errorf("cannot seek pool: %06lu.data", pool->size);

  ensure(fwrite(ptr, pool->size, 1, pool->data) == 1)
    errorf("cannot write pool: %06lu.head", pool->size);
}

void
pool_write (pool_t *pool, off_t item, void *ptr)
{
  mutex_lock(&pool->mutex);
  pool_write_raw(pool, item, ptr);
  mutex_unlock(&pool->mutex);
}

off_t
pool_alloc (pool_t *pool)
{
  mutex_lock(&pool->mutex);

  if (!pool->first)
  {
    pool_flush_cache(pool);
    pool_extend(pool);
    pool_prime_cache(pool);
  }

  void *ptr = malloc(pool->size);
  off_t item = pool->first;

  pool_read_raw(pool, item, ptr);

  pool->first = *((off_t*)ptr);
  //pool_flush(pool);
  free(ptr);

  mutex_unlock(&pool->mutex);
  return item;
}

void
pool_free (pool_t *pool, off_t item)
{
  ensure(item < pool->width)
    errorf("attempt to access item %lu outside pool: %06lu", item, pool->size);

  ensure(item > 0)
    errorf("attempt to access item 0 in pool: %06lu", pool->size);

  void *ptr = malloc(pool->size);
  memset(ptr, 0, pool->size);

  *((off_t*)ptr) = pool->first;
  pool->first = item;

  //pool_flush(pool);
  pool_write_raw(pool, item, ptr);

  free(ptr);

  mutex_unlock(&pool->mutex);
}

int
regmatch (regex_t *re, const char *subject)
{
  return regexec(re, subject, 0, NULL, 0) == 0;
}

typedef int (*ischar)(int);

char*
strskip (char *str, ischar cb)
{
  while (str && *str && cb(*str)) str++;
  return str;
}

char*
strscan (char *str, ischar cb)
{
  while (str && *str && !cb(*str)) str++;
  return str;
}

char*
strtrim (char *str, ischar cb)
{
  char *left = strskip(str, cb);
  size_t len = strlen(left);
  memmove(str, left, len+1);
  for (
    char *p = left + len - 1;
    p >= str && *p && cb(*p);
    *p = 0, p--
  );
  return str;
}

number_t
strtonum(char *str, char **end)
{
  char *p = str;
  number_t n = strtoll(str, &p, 0);

  if (p > str)
  {
    if (*p == 'M')
    {
      n = n * 1024 * 1024;
      p++;
    }
    else
    if (*p == 'G')
    {
      n = n * 1024 * 1024 * 1024;
      p++;
    }
  }

  if (end)
    *end = p;

  return n;
}

int
isalias (int c)
{
  return isalpha(c) || isdigit(c) || strchr("_.@-", c);
}

int
alias_set (char *str, number_t num)
{
  uint32_t hash = 5381;
  for (int i = 0; str[i]; hash = hash * 33 + str[i++]);

  mutex_lock(&alias_mutex);

  alias_t *alias = dict.chains[hash % dict.width];

  for (;
    alias && strcmp(str, alias->str);
    alias = alias->next
  );

  int rc = !alias || (alias && alias->num != num) ? 2: 1;

  if (!alias)
  {
    size_t len = strlen(str);
    alias = allocate(sizeof(alias_t));

    if (!alias) return 0;

    alias->str = allocate(len+1);

    if (!alias->str)
    {
      release(alias, sizeof(alias));
      return 0;
    }

    memmove(alias->str, str, len+1);

    alias->next = dict.chains[hash % dict.width];
    dict.chains[hash % dict.width] = alias;
  }

  alias->num = num;
  mutex_unlock(&alias_mutex);

  return rc;
}

int
alias_get (char *str, number_t *num)
{
  uint32_t hash = 5381;
  for (int i = 0; str[i]; hash = hash * 33 + str[i++]);

  mutex_lock(&alias_mutex);

  alias_t *alias = dict.chains[hash % dict.width];

  for (;
    alias && strcmp(str, alias->str);
    alias = alias->next
  );

  int rc = alias ? 1:0;
  *num = alias ? alias->num: 0;

  mutex_unlock(&alias_mutex);

  return rc;
}

pair_t*
pair_first (record_t *record, pair_t *pair)
{
  if (record->pairs)
  {
    pool_read(&pool_pair, record->pairs, pair);
    return pair;
  }
  return NULL;
}

pair_t*
pair_next (pair_t *pair)
{
  if (pair && pair->sibling)
  {
    pool_read(&pool_pair, pair->sibling, pair);
    return pair;
  }
  return NULL;
}

int
pair_insert (record_t *record, number_t key, number_t val)
{
  pair_t _pair, *pair = pair_first(record, &_pair);

  while (pair && pair->key != key)
    pair = pair_next(pair);

  if (pair && pair->val == val)
    return 2;

  if (pair)
  {
    pair->val = val;
    pool_write(&pool_pair, pair->offset, pair);
    return 1;
  }

  pair = &_pair;
  pair->key = key;
  pair->val = val;
  pair->offset = pool_alloc(&pool_pair);

  pair->sibling = record->pairs;
  record->pairs = pair->offset;

  pool_write(&pool_pair, pair->offset, pair);

  return 1;
}

int
pair_delete (record_t *record, number_t key)
{
  pair_t pair1, pair2, *pair = pair_first(record, &pair1);
  memset(&pair2, 0, sizeof(pair_t));

  while (pair && pair->key != key)
  {
    memmove(&pair2, &pair1, sizeof(pair_t));
    pair = pair_next(pair);
  }
  if (pair)
  {
    pair2.sibling = pair->sibling;
    pool_write(&pool_pair, pair2.offset, &pair2);
    pool_free(&pool_pair, pair->offset);
    return 1;
  }
  return 0;
}

record_t*
record_get (number_t id)
{
  record_t *record = store.chains[id % store.width];

  while (record && record->id != id)
    record = record->next;

  return record;
}

record_t*
record_set (number_t id)
{
  record_t *record = allocate(sizeof(record_t));
  if (record)
  {
    record->id    = id;
    record->pairs = 0;
    record->next  = store.chains[id % store.width];
    record->link  = NULL;
    store.chains[id % store.width] = record;

    if (store.most && record->id > store.most->id)
    {
      store.most->link = record;
    }
    else
    {
      record_t **prev = &store.least;
      for (;
        *prev && (*prev)->id < id;
        prev = &(*prev)->link
      );
      record->link = *prev;
      *prev = record;
    }
    if (!record->link)
    {
      store.most = record;
    }
  }
  return record;
}

int
record_delete(number_t id)
{
  record_t **prev = &store.least;
  for (;
    *prev && (*prev)->id != id;
    prev = &(*prev)->link
  );
  if (*prev)
    *prev = (*prev)->link;

  prev = &(store.chains[id % store.width]);

  while (*prev && (*prev)->id != id)
    prev = &((*prev)->next);

  if (*prev)
  {
    record_t *record = *prev;
    *prev = record->next;

    if (record == store.most)
    {
      store.most = NULL;
    }

    release(record, sizeof(record_t));
    return 1;
  }
  return 0;
}

record_t*
record_get_within (number_t id, number_t limit)
{
  record_t *record = NULL;

  if (!store.least || store.least->id > limit)
    return NULL;

  if (store.least->id >= id && store.least->id <= limit)
    return store.least;

  for (
    number_t i = id;
    !record && i <= limit;
    record = record_get(i), i++
  );
  return record;
}

int
trywrite (int fd, void *buffer, size_t length)
{
  size_t bytes = 0;
  for (int i = 0; i < 3 && bytes < length; i++)
  {
    int written = write(fd, buffer + bytes, length - bytes);
    if (written < 1) break;
    bytes += written;
  }
  return bytes == length;
}

void
respondf (const char *pattern, ...)
{
  if (self->response)
  {
    char buffer[255];
    va_list args;
    va_start(args, pattern);
    vsnprintf(buffer, 255-1, pattern, args); // \n\0
    int len = strlen(buffer);
    if (!strchr(buffer, '\n') && !strcmp("\\n", &pattern[strlen(pattern)-2]))
    {
      buffer[len++] = '\n';
      buffer[len] = 0;
    }
    trywrite(self->response, buffer, len);
    va_end(args);
  }
}

void
activityf(const char *pattern, ...)
{
  mutex_lock(&activity_mutex);

  if (activity)
  {
    va_list args;
    va_start(args, pattern);
    vfprintf(activity, pattern, args);
    va_end(args);
    fputc('\n', activity);
    fflush(activity);
  }

  mutex_unlock(&activity_mutex);
}

int
magic_get (char *name, number_t *num)
{
  if (!strcmp(name, "now"))
  {
    *num = (number_t)time(NULL);
    return 1;
  }
  return 0;
}

int
parse_number (char **line, number_t *number, char *buffer)
{
  char *cursor = strskip(*line, isspace);

  if (isdigit(*cursor))
  {
    *number = strtoll(cursor, line, 0);
    return *line > cursor;
  }

  if (isalpha(*cursor))
  {
    char _buffer[ALIAS];

    if (!buffer)
      buffer = _buffer;

    memset(buffer, 0, ALIAS);

    for (
      size_t i = 0;
      i < ALIAS-1 && *cursor && isalias(*cursor);
      buffer[i] = *cursor++, i++
    );

    if (alias_get(buffer, number) || magic_get(buffer, number))
    {
      *line = cursor;
      return 1;
    }
  }
  return 0;
}

void
parse_insert (char *line)
{
  number_t id, key, val;

  if (!parse_number(&line, &id, NULL))
  {
    respondf("%u expected id: %s\n", E_PARSE, line);
    return;
  }

  while (*line)
  {
    line = strskip(line, isspace);
    if (!*line) break;

    if (!parse_number(&line, &key, NULL) || !parse_number(&line, &val, NULL))
    {
      respondf("%u expected key and val: %s\n", E_PARSE, line);
      return;
    }

    rwlock_wrlock(&rwlock);
    record_t *record = record_get(id);

    if (!record)
      record = record_set(id);

    if (record)
    {
      int rc = pair_insert(record, key, val);
      if (rc)
      {
        if (rc == 1)
          activityf("%u %lu %lu %lu", O_INSERT, id, key, val);

        rwlock_unlock(&rwlock);
        continue;
      }
      if (!record->pairs)
      {
        record_delete(id);
      }
    }

    rwlock_unlock(&rwlock);
    respondf("%u %s %d\n", E_SERVER, __func__, __LINE__);
    return;
  }

  respondf("%u\n", E_OK);
}

void
fields_release (query_t *query)
{
  while (query->fields)
  {
    field_t *field = query->fields;
    field_t *next  = field->next;

    while (field->fkeys)
    {
      field_key_t *fk = field->fkeys;
      field_key_t *fknext = fk->next;

      release(fk, sizeof(field_key_t));

      field->fkeys = fknext;
    }

    release(field, sizeof(field_t));

    query->fields = next;
  }
}

int
field_zero (query_t *query, field_t *field)
{
  field->null = 1;
  field->val = 0;
  field->sum = 0;
  field->min = 0;
  field->max = 0;
  field->diff = 0;
  field->count = 0;
  return E_OK;
}

int
field_noop (query_t *query, field_t *field)
{
  return E_OK;
}

int
field_process_noop (query_t *query, field_t *field, field_key_t *fk, record_t *record, pair_t *pair)
{
  return E_OK;
}

int
field_sum (query_t *query, field_t *field, field_key_t *fk, record_t *record, pair_t *pair)
{
  field->sum += pair->val;
  field->count++;
  field->null = 0;
  return E_OK;
}

int
field_sum_cleanup (query_t *query, field_t *field)
{
  field->val = field->sum;
  return E_OK;
}

int
field_max (query_t *query, field_t *field, field_key_t *fk, record_t *record, pair_t *pair)
{
  field->max = field->count++ == 0 ? pair->val: (field->max > pair->val ? field->max: pair->val);
  field->null = 0;
  return E_OK;
}

int
field_max_cleanup (query_t *query, field_t *field)
{
  field->val = field->max;
  return E_OK;
}

int
field_min (query_t *query, field_t *field, field_key_t *fk, record_t *record, pair_t *pair)
{
  field->min = field->count++ == 0 ? pair->val: (field->max < pair->val ? field->max: pair->val);
  field->null = 0;
  return E_OK;
}

int
field_min_cleanup (query_t *query, field_t *field)
{
  field->val = field->min;
  return E_OK;
}

int
field_first (query_t *query, field_t *field, field_key_t *fk, record_t *record, pair_t *pair)
{
  if (!field->count++) field->val = pair->val;
  field->null = 0;
  return E_OK;
}

int
field_last (query_t *query, field_t *field, field_key_t *fk, record_t *record, pair_t *pair)
{
  field->val = pair->val;
  field->count++;
  field->null = 0;
  return E_OK;
}

int
field_mean (query_t *query, field_t *field, field_key_t *fk, record_t *record, pair_t *pair)
{
  field->sum += pair->val;
  field->count++;
  field->null = 0;
  return E_OK;
}

int
field_mean_cleanup (query_t *query, field_t *field)
{
  field->val = field->sum / (field->count ? field->count: 1);
  return E_OK;
}

int
field_median (query_t *query, field_t *field, field_key_t *fk, record_t *record, pair_t *pair)
{
  field->min = field->count == 0 ? pair->val: (field->max < pair->val ? field->max: pair->val);
  field->max = field->count == 0 ? pair->val: (field->max > pair->val ? field->max: pair->val);
  field->count++;
  field->null = 0;
  return E_OK;
}

int
field_median_cleanup (query_t *query, field_t *field)
{
  field->val = field->max - ((field->max - field->min) / 2);
  return E_OK;
}

int
field_diff (query_t *query, field_t *field, field_key_t *fk, record_t *record, pair_t *pair)
{
  if (field->fkeys == fk)
  {
    field->sum += pair->val;
    field->count++;
  }
  if (field->fkeys != fk)
  {
    field->diff += pair->val;
  }
  field->null = 0;
  return E_OK;
}

int
field_diff_cleanup (query_t *query, field_t *field)
{
  int n = field->count ? field->count: 1;
  number_t s = field->sum / n;
  number_t d = field->diff / n;
  field->val = s > d ? s - d: 0;
  return E_OK;
}

field_t*
field_create (query_t *query)
{
  field_t *field = allocate(sizeof(field_t));
  if (!field) return NULL;

  memset(field, 0, sizeof(field_t));

  field_t **prev = &query->fields;
  while (*prev) prev = &((*prev)->next);
  *prev = field;

  field->prepare = field_zero;
  field->process = field_first;
  field->cleanup = field_noop;

  return field;
}

field_key_t*
field_key_create (field_t *field)
{
  field_key_t *fk = allocate(sizeof(field_key_t));
  if (!fk) return NULL;

  memset(fk, 0, sizeof(field_key_t));

  field_key_t **prev = &field->fkeys;
  while (*prev) prev = &((*prev)->next);
  *prev = fk;

  return fk;
}

int
respond_row (query_t *query)
{
  respondf("%lu", query->id);

  for (field_t *field = query->fields; field; field = field->next)
  {
    char val[25];
    sprintf(val, "%lu", field->val);

    if (field->null)
      sprintf(val, "null");

    if (field->alias[0])
    {
      respondf(" %s %s", field->alias, val);
      continue;
    }
    respondf(" %lu %s", field->fkeys->key, val);
  }

  respondf("\n");
  return E_OK;
}

void
parse_select (char *line)
{
  rwlock_rdlock(&rwlock);

  query_t select, *query =& select;
  memset(query, 0, sizeof(query_t));

  query->handler = respond_row;
  query->filled = 0;

  line = strskip(line, isspace);

  // field list
  while (line && *line)
  {
    if (isspace(*line))
    {
      line = strskip(line, isspace);
      continue;
    }

    if (!strncmp("* ", line, 2))
    {
      query->all_fields = 1;
      line += 2;
      continue;
    }

    if (!strncmp("from ", line, 5))
    {
      line += 5;
      break;
    }

    // as <name>
    if (query->fields && regmatch(&re_field_as, line))
    {
      line += 3;

      field_t *field = query->fields;
      while (field && field->next) field = field->next;

      memset(field->alias, 0, ALIAS);

      char *d = field->alias, *s = line;
      for (;
        *s && isalias(*s) && s - line < ALIAS-1;
        *d++ = *s++
      );
      if (s - line == ALIAS-1 && isalias(*s))
      {
        respondf("%u alias max length %u at: %s\n", E_PARSE, ALIAS-1, line);
        goto done;
      }

      line = s;
      continue;
    }

    // normal field, same as first()
    if (regmatch(&re_field, line))
    {
      field_t *field = field_create(query);
      if (!field) goto res_fail;

      field_key_t *fk = field_key_create(field);
      if (!fk) goto res_fail;

      if (!parse_number(&line, &fk->key, fk->alias))
        goto key_fail;

      continue;
    }

    // max, min, sum, first, last, mean, median, diff
    if (regmatch(&re_field_aggr, line))
    {
      field_t *field = field_create(query);
      if (!field) goto res_fail;

      if (!strncmp("sum(", line, 4))
      {
        field->process = field_sum;
        field->cleanup = field_sum_cleanup;
        line += 4;
      }
      else
      if (!strncmp("min(", line, 4))
      {
        field->process = field_min;
        field->cleanup = field_min_cleanup;
        line += 4;
      }
      else
      if (!strncmp("max(", line, 4))
      {
        field->process = field_max;
        field->cleanup = field_max_cleanup;
        line += 4;
      }
      else
      if (!strncmp("first(", line, 6))
      {
        field->process = field_first;
        line += 6;
      }
      else
      if (!strncmp("last(", line, 5))
      {
        field->process = field_last;
        line += 5;
      }
      else
      if (!strncmp("mean(", line, 5))
      {
        field->process = field_mean;
        field->cleanup = field_mean_cleanup;
        line += 5;
      }
      else
      if (!strncmp("median(", line, 7))
      {
        field->process = field_median;
        field->cleanup = field_median_cleanup;
        line += 7;
      }
      else
      if (!strncmp("diff(", line, 5))
      {
        field->process = field_diff;
        field->cleanup = field_diff_cleanup;
        line += 5;
      }

      while (isalias(*line))
      {
        field_key_t *fk = field_key_create(field);
        if (!fk) goto res_fail;

        if (!parse_number(&line, &fk->key, fk->alias))
          goto key_fail;

        if (*line == ',')
          line++;
      }

      line++;
      continue;
    }

    break;
  }

  // from low:high[:step] [fill]
  while (line && *line)
  {
    if (isspace(*line))
    {
      line = strskip(line, isspace);
      continue;
    }

    if (regmatch(&re_range, line))
    {
      if (!parse_number(&line, &query->low, NULL))
        goto id_fail;

      line++;

      if (!parse_number(&line, &query->high, NULL))
        goto id_fail;

      query->step = 1;

      if (*line == ':')
      {
        line++;

        if (!parse_number(&line, &query->step, NULL))
          goto val_fail;

        query->explicit_step = 1;
      }

      if (!strncmp(" fill", line, 5))
      {
        query->filled = 1;
        line += 5;
      }

      query->have_range = 1;
    }

    break;
  }

  // where clauses
  while (line && *line)
  {
    if (isspace(*line))
    {
      line = strskip(line, isspace);
      continue;
    }

    if (!strncmp("where ", line, 6))
    {
      line += 6;
      continue;
    }

    if (!strncmp("and ", line, 4))
    {
      line += 4;
      continue;
    }

    // parse field
    // operation
    // arg list

    break;
  }

  if (*line)
  {
    respondf("%u unknown syntax: %s\n", E_PARSE, line);
    goto done;
  }

  if (!query->have_range)
  {
    respondf("%u missing id range\n", E_PARSE);
    goto done;
  }

  if (query->low > query->high)
  {
    respondf("%u invalid range: %lu:%lu:%lu\n", E_PARSE, query->low, query->high, query->step);
    goto done;
  }

  // Range query, no gaps, multi-row aggregation
  if (query->filled)
  {
    respondf("%u %lu\n", E_OK, (query->high - query->low) / query->step + 1);

    for (number_t id = query->low; id <= query->high; id += query->step)
    {
      query->count = 0;
      query->id = id;

      number_t step_limit = id + query->step - 1;
      number_t find_limit = query->high < step_limit ? query->high: step_limit;
      record_t *record = record_get_within(id, find_limit);

      for (field_t *field = query->fields; field; field = field->next)
      {
        field->prepare(query, field);
      }

      for (; record && record->id <= query->high && record->id < id + query->step; record = record->link)
      {
        for (field_t *field = query->fields; field; field = field->next)
        {
          for (field_key_t *fk = field->fkeys; fk; fk = fk->next)
          {
            pair_t _pair;
            for (pair_t *pair = pair_first(record, &_pair); pair; pair = pair_next(pair))
            {
              if (pair->key == fk->key)
              {
                field->process(query, field, fk, record, pair);
              }
            }
          }
        }
      }

      for (field_t *field = query->fields; field; field = field->next)
      {
        field->cleanup(query, field);
      }

      query->handler(query);
    }
  }
  else
  // Range query, possible gaps, multi-row aggregation
  if (query->explicit_step)
  {
    query->count = 0;

    for (number_t id = query->low; id <= query->high; id += query->step)
    {
      number_t step_limit = id + query->step - 1;
      number_t find_limit = query->high < step_limit ? query->high: step_limit;
      record_t *record = record_get_within(id, find_limit);

      if (!record)
        continue;

      query->count++;
    }

    respondf("%u %lu\n", E_OK, query->count);

    for (number_t id = query->low; id <= query->high; id += query->step)
    {
      query->count = 0;
      query->id = id;

      number_t step_limit = id + query->step - 1;
      number_t find_limit = query->high < step_limit ? query->high: step_limit;
      record_t *record = record_get_within(id, find_limit);

      if (!record)
        continue;

      for (field_t *field = query->fields; field; field = field->next)
      {
        field->prepare(query, field);
      }

      for (; record && record->id <= query->high && record->id < id + query->step; record = record->link)
      {
        for (field_t *field = query->fields; field; field = field->next)
        {
          pair_t _pair;
          for (pair_t *pair = pair_first(record, &_pair); pair; pair = pair_next(pair))
          {
            for (field_key_t *fk = field->fkeys; fk; fk = fk->next)
            {
              if (pair->key == fk->key)
              {
                field->process(query, field, fk, record, pair);
              }
            }
          }
        }
      }

      for (field_t *field = query->fields; field; field = field->next)
      {
        field->cleanup(query, field);
      }

      query->handler(query);
    }
  }
  else
  // Simple range query, no multi-row aggregation
  {
    query->count = 0;
    record_t *record = record_get_within(query->low, query->high);

    for (record_t *r = record; r && r->id <= query->high; r = r->link)
    {
      query->count++;
    }

    respondf("%u %lu\n", E_OK, query->count);
    query->count = 0;

    for (; record && record->id <= query->high; record = record->link)
    {
      query->count = 0;
      query->id = record->id;

      if (query->all_fields)
      {
        fields_release(query);

        pair_t _pair;
        for (pair_t *pair = pair_first(record, &_pair); pair; pair = pair_next(pair))
        {
          field_t *field  = field_create(query);
          if (!field) goto res_fail;

          field_key_t *fk = field_key_create(field);
          if (!fk) goto res_fail;

          fk->key = pair->key;
        }
      }

      for (field_t *field = query->fields; field; field = field->next)
      {
        field->prepare(query, field);

        pair_t _pair;
        for (pair_t *pair = pair_first(record, &_pair); pair; pair = pair_next(pair))
        {
          for (field_key_t *fk = field->fkeys; fk; fk = fk->next)
          {
            if (pair->key == fk->key)
            {
              field->process(query, field, fk, record, pair);
            }
          }
        }
        field->cleanup(query, field);
      }
      query->handler(query);
    }
  }

  goto done;

res_fail:
  respondf("%u insufficient resources\n", E_SERVER, line);
  goto done;

id_fail:
  respondf("%u expected id at: %s\n", E_PARSE, line);
  goto done;

key_fail:
  respondf("%u expected key at: %s\n", E_PARSE, line);
  goto done;

val_fail:
  respondf("%u expected val at: %s\n", E_PARSE, line);
  goto done;

done:
  fields_release(query);
  rwlock_unlock(&rwlock);
}

void
parse_delete (char *line)
{
  rwlock_wrlock(&rwlock);
  int deleted_records = 0;
  int deleted_pairs = 0;
  int field_count = 0;

  query_t delete, *query = &delete;
  memset(&delete, 0, sizeof(query_t));

  while (line && *line)
  {
    if (isspace(*line))
    {
      line = strskip(line, isspace);
      continue;
    }

    if (!strncmp("from ", line, 5))
    {
      line += 5;
      break;
    }

    if (regmatch(&re_field, line))
    {
      field_t *field = field_create(query);
      if (!field) goto res_fail;

      field_key_t *fk = field_key_create(field);
      if (!fk) goto res_fail;

      if (!parse_number(&line, &fk->key, fk->alias))
        goto key_fail;

      field_count++;
    }

    break;
  }

  while (line && *line)
  {
    if (isspace(*line))
    {
      line = strskip(line, isspace);
      continue;
    }

    if (regmatch(&re_range, line))
    {
      if (!parse_number(&line, &query->low, NULL))
        goto id_fail;

      line++;

      if (!parse_number(&line, &query->high, NULL))
        goto id_fail;

      if (*line && !isspace(*line))
        goto syn_fail;

      record_t *record = record_get_within(query->low, query->high);

      while (record && record->id >= query->low && record->id <= query->high)
      {
        record_t *next = record->link;

        pair_t _pair;
        for (pair_t *pair = pair_first(record, &_pair); pair; pair = pair_next(pair))
        {
          int kill = 1;
          if (query->fields)
          {
            kill = 0;
            for (field_t *field = query->fields; !kill && field; field = field->next)
              kill = (field->fkeys->key == pair->key);
          }
          if (kill)
          {
            pair_delete(record, pair->key);
            deleted_pairs++;
          }
        }
        if (!record->pairs)
        {
          record_delete(record->id);
          deleted_records++;
        }
        record = next;
      }

      if (deleted_records || deleted_pairs)
      {
        int limit = field_count * 25 + 255, length = 0;
        char scratch[limit]; scratch[0] = 0;

        for (field_t *field = query->fields; field; field = field->next)
          length += sprintf(scratch + length, " %lu", field->fkeys->key);

        activityf("2%s %lu:%lu", scratch, query->low, query->high);
      }
      break;
    }

    goto id_fail;
  }

  respondf("%u %lu %lu\n", E_OK, deleted_records, deleted_pairs);
  goto done;

syn_fail:
  respondf("%u unexpected syntax\n", E_PARSE, line);
  goto done;

res_fail:
  respondf("%u insufficient resources\n", E_SERVER, line);
  goto done;

id_fail:
  respondf("%u expected id at: %s\n", E_PARSE, line);
  goto done;

key_fail:
  respondf("%u expected key at: %s\n", E_PARSE, line);
  goto done;

done:
  fields_release(query);
  rwlock_unlock(&rwlock);
}

void
parse_alias (char *line)
{
  rwlock_wrlock(&rwlock);

  if (regmatch(&re_alias_get, line))
  {
    number_t num = 0;
    int rc = alias_get(line, &num);
    respondf("%u %lu\n", rc ? E_OK: E_MISSING, num);
    goto done;
  }

  if (regmatch(&re_alias_set, line))
  {
    number_t num = strtoll(line, &line, 0);
    line = strskip(line, isspace);

    int rc = alias_set(line, num);

    if (rc > 0)
    {
      respondf("%u %lu\n", E_OK, num);
      goto done;
    }

    respondf("%u alias failed\n", E_SERVER);
    goto done;
  }

  respondf("%u unknown syntax: %s\n", E_PARSE, line);

done:
  rwlock_unlock(&rwlock);
}

void
parse_match (char *line)
{
  rwlock_rdlock(&rwlock);

  regex_t re;

  if (regcomp(&re, line, REG_EXTENDED|REG_NOSUB) != 0)
  {
    respondf("%u invalid regex\n", E_PARSE);
    goto done;
  }

  number_t matches = 0;

  for (size_t i = 0; i < dict.width; i++)
  {
    for (alias_t *alias = dict.chains[i]; alias; alias = alias->next)
    {
      if (regmatch(&re, alias->str))
        matches++;
    }
  }

  respondf("%u %lu\n", E_OK, matches);

  for (size_t i = 0; i < dict.width; i++)
  {
    for (alias_t *alias = dict.chains[i]; alias; alias = alias->next)
    {
      if (regmatch(&re, alias->str))
        respondf("%lu %s\n", alias->num, alias->str);
    }
  }

  regfree(&re);

done:
  rwlock_unlock(&rwlock);
}

void
status ()
{
  number_t records = 0, aliases = 0;

  respondf("%u 1\n", E_OK);

  for (
    record_t *record = store.least;
    record;
    record = record->link, records++
  );

  for (size_t i = 0; i < dict.width; i++)
  {
    for (
      alias_t *alias = dict.chains[i];
      alias;
      alias = alias->next, aliases++
    );
  }

  respondf("records %lu", records);
  respondf(" aliases %lu", aliases);
  respondf(" mem_used %lu", state.mem_used);
  respondf(" mem_limit %lu", state.mem_limit);
  respondf(" mem_limit_hit %lu", state.mem_limit_hit);
  respondf(" max_packet %u", (uint32_t)state.max_packet);
  respondf(" path %s", state.data_path);
  respondf(" socket %s", state.sock_path);

  number_t chains_avg, chains_min, chains_max;

  chains_avg = 0;
  chains_min = 0;
  chains_max = 0;

  for (uint chain = 0; chain < store.width; chain++)
  {
    number_t len = 0;
    for (record_t *record = store.chains[chain]; record;
      record = record->next, len++
    );

    chains_avg += len;
    if (!chain || len < chains_min) chains_min = len;
    if (!chain || len > chains_max) chains_max = len;
  }

  chains_avg /= store.width;

  respondf(" record_chains_avg %lu", chains_avg);
  respondf(" record_chains_min %lu", chains_min);
  respondf(" record_chains_max %lu", chains_max);

  chains_avg = 0;
  chains_min = 0;
  chains_max = 0;

  for (uint chain = 0; chain < dict.width; chain++)
  {
    number_t len = 0;
    for (alias_t *alias = dict.chains[chain]; alias;
      alias = alias->next, len++
    );

    chains_avg += len;
    if (!chain || len < chains_min) chains_min = len;
    if (!chain || len > chains_max) chains_max = len;
  }

  chains_avg /= store.width;

  respondf(" alias_chains_avg %lu", chains_avg);
  respondf(" alias_chains_min %lu", chains_min);
  respondf(" alias_chains_max %lu", chains_max);

  respondf(" pool_size %lu", pool_pair.size);
  respondf(" pool_width %lu", pool_pair.width);
  respondf("\n");
}

void
consolidate ()
{
  int rc = E_OK;
  char scratch[PATH];

  mutex_lock(&activity_mutex);

  fclose(activity);

  snprintf(scratch, sizeof(scratch), "%s/activity", state.data_path);
  activity = fopen(scratch, "w");

  if (!activity)
  {
    rc = E_SERVER;
    errorf("failed to repopen %s", scratch);
    goto done;
  }

  for (record_t *record = store.least; record; record = record->link)
  {
    if (record->pairs)
    {
      fprintf(activity, "1 %lu", record->id);
      pair_t _pair;
      for (pair_t *pair = pair_first(record, &_pair); pair; pair = pair_next(pair))
        fprintf(activity, " %lu %lu", pair->key, pair->val);
      fprintf(activity, "\n");
    }
  }
  fflush(activity);

  snprintf(scratch, sizeof(scratch), "%s/aliases", state.data_path);
  FILE *aliases = fopen(scratch, "w");

  if (!aliases)
  {
    rc = E_SERVER;
    errorf("failed to repopen %s", scratch);
    goto done;
  }

  for (size_t i = 0; aliases && i < dict.width; i++)
  {
    for (alias_t *alias = dict.chains[i]; alias; alias = alias->next)
      fprintf(aliases, "4 %lu %s\n", alias->num, alias->str);
  }
  fclose(aliases);

done:
  mutex_unlock(&activity_mutex);
  respondf("%u\n", rc);
}

void
parse (char *line)
{
  line = strtrim(line, isspace);

  if (!strncmp("1 ", line, 2))
  {
    parse_insert(line + 2);
  }
  else
  if (!strncmp("insert ", line, 7))
  {
    parse_insert(line + 7);
  }
  else
  if (!strncmp("2 ", line, 2))
  {
    parse_delete(line + 2);
  }
  else
  if (!strncmp("delete ", line, 7))
  {
    parse_delete(line + 7);
  }
  else
  if (!strncmp("3 ", line, 2))
  {
    parse_select(line + 2);
  }
  else
  if (!strncmp("select ", line, 7))
  {
    parse_select(line + 7);
  }
  else
  if (!strncmp("4 ", line, 2))
  {
    parse_alias(line + 2);
  }
  else
  if (!strncmp("alias ", line, 6))
  {
    parse_alias(line + 6);
  }
  else
  if (!strncmp("match ", line, 6))
  {
    parse_match(line + 6);
  }
  else
  if (!strcmp("status", line))
  {
    status();
  }
  else
  if (!strcmp("consolidate", line))
  {
    consolidate();
  }
  else
  {
    respondf("%u unknown: %s\n", E_PARSE, line);
  }
}

void*
client (void *ptr)
{
  self_t _self;
  pthread_setspecific(keyself, &_self);

  self->response = *((int*)ptr);

  char *packet = allocate(state.max_packet);

  if (!packet)
  {
    respondf("%u oom\n", E_SERVER);
    goto done;
  }

  errno = 0;

  for (;;)
  {
    char *cursor = packet;

    for (;
      read(self->response, cursor, 1) == 1 && *cursor && *cursor != '\n' && cursor < &packet[state.max_packet-1];
      cursor++
    );

    *cursor = 0;

    if (!errno && packet < cursor)
    {
      parse(packet);
      continue;
    }

    break;
  }

done:

  close(self->response);
  release(packet, state.max_packet);

  *((int*)ptr) = -1;

  return NULL;
}

int
main (int argc, char *argv[])
{
  char scratch[PATH];
  multithreaded = 0;

  store.width = PRIME_10000;
  dict.width  = PRIME_1000;

  state.mem_limit   = 1024 * 1024 * 1024;
  state.max_packet  = 1024 * 1024 * 1;
  state.max_threads = 16;

  long page_size  = sysconf(_SC_PAGESIZE);
  long phys_pages = sysconf(_SC_PHYS_PAGES);

  if (page_size > 0 && phys_pages > 0)
  {
    number_t total_mem = (number_t)phys_pages * (number_t)page_size;

    if (total_mem > 1024 * 1024 * 1024)
    {
      store.width = PRIME_100000;
      dict.width  = PRIME_10000;
    }

    state.mem_limit = total_mem * 0.75;
  }

  snprintf(state.sock_path, sizeof(state.sock_path), "/tmp/n3.sock");
  snprintf(state.data_path, sizeof(state.data_path), ".");

  pthread_rwlock_init(&rwlock, NULL);
  pthread_mutex_init(&state_mutex, NULL);
  pthread_mutex_init(&alias_mutex, NULL);
  pthread_mutex_init(&activity_mutex, NULL);
  pthread_key_create(&keyself, NULL);

  self_t _self;
  pthread_setspecific(keyself, &_self);
  signal(SIGPIPE, SIG_IGN);

//  if (!pool_open(&pool_pair, sizeof(pair_t)))
//  {
    pool_init(sizeof(pair_t));
    ensure(pool_open(&pool_pair, sizeof(pair_t)))
      errorf("unable to open pair_t pool");
//  }

  for (int i = 1; i < argc; i++)
  {
    if (!strcmp("-path", argv[i]))
    {
      snprintf(state.data_path, sizeof(state.data_path), "%s", argv[++i]);
      continue;
    }
    if (!strcmp("-socket", argv[i]))
    {
      snprintf(state.sock_path, sizeof(state.sock_path), "%s", argv[++i]);
      continue;
    }
    if (!strcmp("-memory", argv[i]))
    {
      state.mem_limit = strtonum(argv[++i], NULL);
      continue;
    }
    if (!strcmp("-width", argv[i]))
    {
      store.width = strtoll(argv[++i], NULL, 0);
      dict.width  = store.width / 10;
      continue;
    }
    if (!strcmp("-threads", argv[i]))
    {
      state.max_threads = strtoll(argv[++i], NULL, 0);
      continue;
    }
  }

  store.chains = allocate(sizeof(record_t*) * store.width);
  dict.chains  = allocate(sizeof(alias_t*)  * dict.width);

  for (uint64_t i = 0; i < store.width; i++)
    store.chains[i] = NULL;

  for (uint64_t i = 0; i < dict.width; i++)
    dict.chains[i] = NULL;

  ensure(regcomp(&re_range, RE_RANGE, REG_EXTENDED|REG_NOSUB) == 0)
    errorf("regcomp failed: %s", RE_RANGE);

  ensure(regcomp(&re_where, RE_WHERE, REG_EXTENDED|REG_NOSUB) == 0)
    errorf("regcomp failed: %s", RE_WHERE);

  ensure(regcomp(&re_field, RE_FIELD, REG_EXTENDED|REG_NOSUB) == 0)
    errorf("regcomp failed: %s", RE_FIELD);

  ensure(regcomp(&re_field_aggr, RE_FIELD_AGGR, REG_EXTENDED|REG_NOSUB) == 0)
    errorf("regcomp failed: %s", RE_FIELD_AGGR);

  ensure(regcomp(&re_field_as, RE_FIELD_AS, REG_EXTENDED|REG_NOSUB) == 0)
    errorf("regcomp failed: %s", RE_FIELD_AS);

  ensure(regcomp(&re_alias_set, RE_ALIAS_SET, REG_EXTENDED|REG_NOSUB) == 0)
    errorf("regcomp failed: %s", RE_ALIAS_SET);

  ensure(regcomp(&re_alias_get, RE_ALIAS_GET, REG_EXTENDED|REG_NOSUB) == 0)
    errorf("regcomp failed: %s", RE_ALIAS_GET);

  char *packet = allocate(state.max_packet);

  self->response = 0;
  activity = NULL;

  snprintf(scratch, sizeof(scratch), "%s/activity", state.data_path);
  FILE *replay = fopen(scratch, "r");

  if (replay)
  {
    errorf("replay %s", scratch);
    while (fgets(packet, state.max_packet, replay))
      parse(packet);
    fclose(replay);
  }

  snprintf(scratch, sizeof(scratch), "%s/activity", state.data_path);

  ensure((activity = fopen(scratch, "a+")) && activity)
    errorf("missing %s", scratch);

  snprintf(scratch, sizeof(scratch), "%s/aliases", state.data_path);

  FILE *aliases = fopen("aliases", "r");

  if (aliases)
  {
    errorf("parse %s", scratch);
    while (fgets(packet, state.max_packet, aliases))
      parse(packet);
    fclose(aliases);
  }

  self->response = fileno(stdout);

  release(packet, state.max_packet);

  int sock_fd = socket(AF_UNIX, SOCK_STREAM, 0);

  ensure (sock_fd >= 0)
    errorf("socket failed");

  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, state.sock_path, sizeof(addr.sun_path) - 1);

  unlink(state.sock_path);

  ensure(bind(sock_fd, (struct sockaddr*)&addr, sizeof(addr)) == 0)
    errorf("bind failed");

  ensure(listen(sock_fd, 32) == 0)
    errorf("listen failed");

  errorf("ready");

  multithreaded = 1;

  int thread_fds[state.max_threads];
  memset(thread_fds, 0, sizeof(thread_fds));

  pthread_t threads[state.max_threads];
  memset(threads, 0, sizeof(threads));

  for (;;)
  {
    int fd = accept(sock_fd, NULL, NULL);
    if (fd >= 0)
    {
      int created = 0;
      for (int i = 0; !created && i < state.max_threads; i++)
      {
        if (thread_fds[i] == -1)
        {
          ensure(pthread_join(threads[i], NULL) == 0)
            errorf("thread join");
          thread_fds[i] = 0;
        }
      }
      for (int i = 0; !created && i < state.max_threads; i++)
      {
        if (!thread_fds[i])
        {
          thread_fds[i] = fd;

          int rc = pthread_create(&threads[i], NULL, client, &thread_fds[i]);

          if (rc)
          {
            errorf("%u thread create: %d", E_SERVER, rc);
            thread_fds[i] = 0;
            break;
          }
          created = 1;
        }
      }
      if (!created)
      {
        self->response = fd;
        respondf("%u max threads\n", E_SERVER);
        close(fd);
      }
    }
  }

  fclose(activity);
  close(sock_fd);

  regfree(&re_range);
  regfree(&re_where);
  regfree(&re_field);
  regfree(&re_field_aggr);
  regfree(&re_field_as);
  regfree(&re_alias_set);
  regfree(&re_alias_get);

  unlink(state.sock_path);

  return EXIT_SUCCESS;
}
