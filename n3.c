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

#define ensure(x) for ( ; !(x) ; exit(EXIT_FAILURE) )

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

typedef struct _pair_t {
  number_t key;
  number_t val;
  struct _pair_t *next;
} pair_t;

typedef struct _record_t {
  number_t id;
  pair_t *pairs;
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
  int slab;
  size_t slab_width_record;
  size_t slab_width_pair;
  size_t max_threads;
} state_t;

typedef struct _slab_t {
  size_t size;
  size_t width;
  int first;
  void *region;
  struct _slab_t *next;
} slab_t;

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
  query_callback handler;
} query_t;

pthread_rwlock_t rwlock;
pthread_mutex_t slab_mutex;
pthread_mutex_t state_mutex;
pthread_mutex_t alias_mutex;
pthread_mutex_t activity_mutex;
static pthread_key_t keyself;
#define self ((self_t*)pthread_getspecific(keyself))

FILE *activity;
store_t store;
state_t state;
dict_t dict;

slab_t *slab_record;
slab_t *slab_pair;

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

slab_t*
slab_create (size_t size, size_t width)
{
  size_t bytes = sizeof(slab_t) + (size * width);
  slab_t *slab = NULL;

  pthread_mutex_lock(&state_mutex);

  if (state.mem_used + bytes >= state.mem_limit)
  {
    if (!state.mem_limit_hit)
      errorf("hit state.mem_limit %lu", state.mem_limit);
    state.mem_limit_hit++;
    goto done;
  }

  slab = malloc(sizeof(slab_t));
  if (!slab) goto done;

  slab->region = malloc(size * width);
  if (!slab->region) { free(slab); slab = NULL; goto done; }

  slab->size  = size;
  slab->width = width;

  state.mem_used += bytes;

  for (size_t i = 0; i < width-1; i++)
    *((int*)(slab->region + (i * size))) = i+1;
  *((int*)(slab->region + ((width-1) * size))) = -1;
  slab->first = 0;

  errorf("slab: %d %d", (int)size, (int)width);

done:
  pthread_mutex_unlock(&state_mutex);
  return slab;
}

void*
slab_allocate (slab_t **slab)
{
  pthread_mutex_lock(&slab_mutex);

  void *ptr = NULL;

  for (;;)
  {
    for (slab_t *s = *slab; s; s = s->next)
    {
      if (s->first >= 0)
      {
        ptr = s->region + (s->first * s->size);
        s->first = *((int*)ptr);
        memset(ptr, 0, s->size);
        goto done;
      }
    }

    slab_t *new = slab_create((*slab)->size, (*slab)->width);

    if (!new)
    {
      ptr = NULL;
      goto done;
    }

    new->next = *slab;
    *slab = new;
  }

done:
  pthread_mutex_unlock(&slab_mutex);
  return ptr;
}

int
slab_release (slab_t **slab, void *ptr)
{
  pthread_mutex_lock(&slab_mutex);

  int rc = 0;

  for (slab_t *s = *slab; s; s = s->next)
  {
    if (ptr >= s->region && ptr <= s->region + s->size * s->width)
    {
      int slot = (ptr - s->region) / s->size;
      *((int*)(s->region + slot * s->size)) = s->first;
      s->first = slot;
      rc = 1;
      goto done;
    }
  }

done:
  pthread_mutex_unlock(&slab_mutex);
  return rc;
}

void*
allocate (size_t bytes)
{
  if (state.slab)
  {
    if (bytes == sizeof(record_t))
      return slab_allocate(&slab_record);

    if (bytes == sizeof(pair_t))
      return slab_allocate(&slab_pair);
  }

  pthread_mutex_lock(&state_mutex);

  if (state.mem_used + bytes >= state.mem_limit)
  {
    if (!state.mem_limit_hit)
      errorf("hit state.mem_limit %lu", state.mem_limit);
    state.mem_limit_hit++;
    pthread_mutex_unlock(&state_mutex);
    return NULL;
  }

  state.mem_used += bytes;

  pthread_mutex_unlock(&state_mutex);

  return malloc(bytes);
}

void
release (void *ptr, size_t bytes)
{
  if (ptr)
  {

    if (state.slab && bytes == sizeof(record_t))
    {
      slab_release(&slab_record, ptr);
      return;
    }

    if (state.slab && bytes == sizeof(pair_t))
    {
      slab_release(&slab_pair, ptr);
      return;
    }

    pthread_mutex_lock(&state_mutex);

    ensure(state.mem_used >= bytes)
      errorf("bad state.mem_used");

    state.mem_used -= bytes;

    pthread_mutex_unlock(&state_mutex);

    free(ptr);
  }
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

  pthread_mutex_lock(&alias_mutex);

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
  pthread_mutex_unlock(&alias_mutex);

  return rc;
}

int
alias_get (char *str, number_t *num)
{
  uint32_t hash = 5381;
  for (int i = 0; str[i]; hash = hash * 33 + str[i++]);

  pthread_mutex_lock(&alias_mutex);

  alias_t *alias = dict.chains[hash % dict.width];

  for (;
    alias && strcmp(str, alias->str);
    alias = alias->next
  );

  int rc = alias ? 1:0;
  *num = alias ? alias->num: 0;

  pthread_mutex_unlock(&alias_mutex);

  return rc;
}

int
pair_insert (record_t *record, number_t key, number_t val)
{
  pair_t *pair = NULL;

  pair = record->pairs;

  while (pair && pair->key != key)
    pair = pair->next;

  if (pair && pair->val == val)
  {
    return 2;
  }

  if (pair)
  {
    pair->val = val;
    return 1;
  }

  pair = allocate(sizeof(pair_t));

  if (pair)
  {
    pair->key = key;
    pair->val = val;

    pair->next = record->pairs;
    record->pairs = pair;

    return 1;
  }

  return 0;
}

int
pair_delete (record_t *record, number_t key)
{
  pair_t **prev = &record->pairs;

  while (*prev && (*prev)->key != key)
    prev = &(*prev)->next;

  if (*prev)
  {
    pair_t *pair = *prev;
    *prev = pair->next;
    release(pair, sizeof(pair_t));
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
    record->pairs = NULL;
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
  pthread_mutex_lock(&activity_mutex);

  if (activity)
  {
    va_list args;
    va_start(args, pattern);
    vfprintf(activity, pattern, args);
    va_end(args);
    fputc('\n', activity);
    fflush(activity);
  }

  pthread_mutex_unlock(&activity_mutex);
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

    if (alias_get(buffer, number))
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

    pthread_rwlock_wrlock(&rwlock);
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

        pthread_rwlock_unlock(&rwlock);
        continue;
      }
      if (!record->pairs)
      {
        record_delete(id);
      }
    }

    pthread_rwlock_unlock(&rwlock);
    respondf("%u %s %d\n", E_SERVER, __func__, __LINE__);
    return;
  }

  respondf("%u\n", E_OK);
}

void
parse_delete (char *line)
{
  pthread_rwlock_wrlock(&rwlock);
  int deleted = 0;

  for (;;)
  {
    line = strskip(line, isspace);
    if (!*line) break;

    if (regmatch(&re_range, line))
    {
      number_t low, high;

      if (!parse_number(&line, &low, NULL))
      {
        respondf("%u expected id at: %s\n", E_PARSE, line);
        goto done;
      }

      line++;

      if (!parse_number(&line, &high, NULL))
      {
        respondf("%u expected id at: %s\n", E_PARSE, line);
        goto done;
      }

      record_t *record = record_get(low);

      if (!record)
      {
        record = store.least;
        while (record && record->id < low)
          record = record->next;
      }

      while (record && record->id >= low && record->id <= high)
      {
        record_t *next = record->next;

        for (pair_t *pair = record->pairs; pair; pair = pair->next)
          pair_delete(record, pair->key);

        record_delete(record->id);
        deleted++;

        record = next;
      }

      if (deleted)
        activityf("2 %lu:%lu", low, high);

      continue;
    }

    respondf("%u unknown syntax at: %s\n", E_PARSE, line);
    goto done;
  }

  respondf("%u %lu\n", E_OK, deleted);

done:
  pthread_rwlock_unlock(&rwlock);
}

int
field_zero (query_t *query, field_t *field)
{
  field->val = 0;
  field->sum = 0;
  field->min = 0;
  field->max = 0;
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
  field->max = field->count == 0 ? pair->val: (field->max > pair->val ? field->max: pair->val);
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
  field->min = field->count == 0 ? pair->val: (field->max < pair->val ? field->max: pair->val);
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
  if (!field->count) field->val = pair->val;
  return E_OK;
}

int
field_last (query_t *query, field_t *field, field_key_t *fk, record_t *record, pair_t *pair)
{
  field->val = pair->val;
  return E_OK;
}

int
field_mean (query_t *query, field_t *field, field_key_t *fk, record_t *record, pair_t *pair)
{
  field->sum += pair->val;
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
  if (field->fkeys == fk) field->sum  += pair->val;
  if (field->fkeys != fk) field->diff += pair->val;
  return E_OK;
}

int
field_diff_cleanup (query_t *query, field_t *field)
{
  field->val = (field->sum - field->diff) / (query->count ? query->count: 1);
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
    if (field->alias)
    {
      respondf(" %s %lu", field->alias, field->val);
      continue;
    }
    respondf(" %lu %lu", field->fkeys->key, field->val);
  }

  respondf("\n");
  return E_OK;
}

void
parse_select (char *line)
{
  pthread_rwlock_rdlock(&rwlock);

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
      record_t *record = NULL;

      for (
        number_t i = id;
        !record && i <= query->high && i < id + query->step;
        record = record_get(i), i++
      );

      for (field_t *field = query->fields; field; field = field->next)
      {
        field->count = 0;
        field->prepare(query, field);
      }

      for (; record && record->id <= query->high && record->id < id + query->step; record = record->link)
      {
        for (field_t *field = query->fields; field; field = field->next)
        {
          for (pair_t *pair = record->pairs; pair; pair = pair->next)
          {
            for (field_key_t *fk = field->fkeys; fk; fk = fk->next)
            {
              if (pair->key == fk->key)
              {
                field->process(query, field, fk, record, pair);
                field->count++;
              }
            }
          }
        }
        query->count++;
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
      record_t *record = NULL;

      for (
        number_t i = id;
        !record && i <= query->high && i < id + query->step;
        record = record_get(i), i++
      );

      if (!record)
        continue;

      query->count++;
    }

    respondf("%u %lu\n", E_OK, query->count);

    for (number_t id = query->low; id <= query->high; id += query->step)
    {
      query->count = 0;
      query->id = id;
      record_t *record = NULL;

      for (
        number_t i = id;
        !record && i <= query->high && i < id + query->step;
        record = record_get(i), i++
      );

      if (!record)
        continue;

      for (field_t *field = query->fields; field; field = field->next)
      {
        field->count = 0;
        field->prepare(query, field);
      }

      for (; record && record->id <= query->high && record->id < id + query->step; record = record->link)
      {
        for (field_t *field = query->fields; field; field = field->next)
        {
          for (pair_t *pair = record->pairs; pair; pair = pair->next)
          {
            for (field_key_t *fk = field->fkeys; fk; fk = fk->next)
            {
              if (pair->key == fk->key)
              {
                field->process(query, field, fk, record, pair);
                field->count++;
              }
            }
          }
        }
        query->count++;
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
    record_t *record = NULL;

    for (
      number_t i = query->low;
      !record && i <= query->high;
      record = record_get(i), i++
    );

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

      for (field_t *field = query->fields; field; field = field->next)
      {
        field->count = 0;
        field->prepare(query, field);

        for (pair_t *pair = record->pairs; pair; pair = pair->next)
        {
          for (field_key_t *fk = field->fkeys; fk; fk = fk->next)
          {
            if (pair->key == fk->key)
            {
              field->process(query, field, fk, record, pair);
              field->count++;
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

  pthread_rwlock_unlock(&rwlock);
}

void
parse_alias (char *line)
{
  pthread_rwlock_wrlock(&rwlock);

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
  pthread_rwlock_unlock(&rwlock);
}

void
parse_match (char *line)
{
  pthread_rwlock_rdlock(&rwlock);

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
  pthread_rwlock_unlock(&rwlock);
}

void
status ()
{
  number_t records = 0, pairs = 0, aliases = 0;

  respondf("%u 1\n", E_OK);

  for (
    record_t *record = store.least;
    record;
    record = record->link, records++
  )
  {
    for (
      pair_t *pair = record->pairs;
      pair;
      pair = pair->next, pairs++
    );
  }

  for (size_t i = 0; i < dict.width; i++)
  {
    for (
      alias_t *alias = dict.chains[i];
      alias;
      alias = alias->next, aliases++
    );
  }

  respondf("records %lu", records);
  respondf(" pairs %lu", pairs);
  respondf(" aliases %lu", aliases);
  respondf(" mem_used %lu", state.mem_used);
  respondf(" mem_limit %lu", state.mem_limit);
  respondf(" mem_limit_hit %lu", state.mem_limit_hit);
  respondf(" max_packet %u", (uint32_t)state.max_packet);
  respondf(" path %s", state.data_path);
  respondf(" socket %s", state.sock_path);
  respondf(" slab %d", state.slab);
  respondf(" slab_width_record %u", (uint32_t)state.slab_width_record);
  respondf(" slab_width_pair %u", (uint32_t)state.slab_width_pair);

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

  respondf("\n");
}

void
consolidate ()
{
  int rc = E_OK;
  char scratch[PATH];

  pthread_mutex_lock(&activity_mutex);

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
      for (pair_t *pair = record->pairs; pair; pair = pair->next)
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
  pthread_mutex_unlock(&activity_mutex);
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

  store.width = 9973;
  dict.width = 997;
  state.mem_limit = 1024 * 1024 * 256;
  state.max_packet = 1024 * 1024 * 1;
  state.max_threads = 16;
  state.slab = 0;
  state.slab_width_record = 100000;
  state.slab_width_pair   = 10000000;

  snprintf(state.sock_path, sizeof(state.sock_path), "/tmp/n3.sock");
  snprintf(state.data_path, sizeof(state.data_path), ".");

  pthread_rwlock_init(&rwlock, NULL);
  pthread_mutex_init(&slab_mutex, NULL);
  pthread_mutex_init(&state_mutex, NULL);
  pthread_mutex_init(&alias_mutex, NULL);
  pthread_mutex_init(&activity_mutex, NULL);
  pthread_key_create(&keyself, NULL);

  self_t _self;
  pthread_setspecific(keyself, &_self);
  signal(SIGPIPE, SIG_IGN);

  for (int i = 1; i < argc; i++)
  {
    if (!strcmp("-slab", argv[i]))
    {
      state.slab = 1;
      continue;
    }
    if (!strcmp("-slabrecord", argv[i]))
    {
      state.slab_width_record = strtoll(argv[++i], NULL, 0);
      continue;
    }
    if (!strcmp("-slabpair", argv[i]))
    {
      state.slab_width_pair = strtoll(argv[++i], NULL, 0);
      continue;
    }
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

  if (state.slab)
  {
    slab_record = slab_create(sizeof(record_t), state.slab_width_record);
    slab_pair   = slab_create(sizeof(pair_t),   state.slab_width_pair);
  }

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
