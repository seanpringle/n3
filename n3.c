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

#define LINE 1024
#define PATH 256
#define ALIAS 128

#define E_OK 0
#define E_PARSE 1
#define E_SERVER 2
#define E_MISSING 3

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

#define F_SUM (1<<0)
#define F_MAX (1<<1)
#define F_MIN (1<<2)
#define F_FIRST (1<<3)
#define F_LAST (1<<4)
#define F_MEAN (1<<5)
#define F_MEDIAN (1<<6)
#define F_DIFF (1<<7)

typedef struct _node_t {
  number_t num;
  struct _node_t *next;
} node_t;

typedef struct _field_t {
  node_t *keys;
  pair_t *col;
  number_t flags;
  number_t count_nodes;
  number_t count_rows;
  number_t sum;
  number_t min;
  number_t max;
  number_t diff1;
  number_t diff2;
  char alias[ALIAS];
  struct _field_t *next;
} field_t;

#define W_EQ (1<<0)
#define W_LT (1<<1)
#define W_GT (1<<2)
#define W_NE (1<<3)
#define W_AND (1<<4)
#define W_OR (1<<5)
#define W_XOR (1<<6)

typedef struct {
  number_t key;
  number_t val;
  number_t flags;
} where_t;

#define MAX_WHERES 32

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

void
release_result (record_t *result)
{
  while (result)
  {
    while (result->pairs)
    {
      pair_t *npair = result->pairs->next;
      release(result->pairs, sizeof(pair_t));
      result->pairs = npair;
    }

    record_t *nresult = result->next;
    release(result, sizeof(record_t));
    result = nresult;
  }
}

void
release_fields (field_t *field)
{
  while (field)
  {
    while (field->keys)
    {
      node_t *next = field->keys->next;
      release(field->keys, sizeof(node_t));
      field->keys = next;
    }
    field_t *next = field->next;
    release(field, sizeof(field_t));
    field = next;
  }
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

int
fetch_range (record_t **results, number_t id, number_t range, number_t step, field_t *field, where_t *wheres, size_t where)
{
  record_t *first = NULL, *result = NULL, *record = NULL;

  int rc = E_SERVER, keep = 0;

  for (
    number_t i = id;
    i <= range && !record;
    record = record_get(i), i++
  );

  number_t steps = 0;

  for (; record && record->id <= range; record = record->link)
  {
    keep = (steps++ % step) == 0;

    // if fields supplied, only retrieve records containing them
    if (keep && field)
    {
      keep = 0;
      for (pair_t *pair = record->pairs; !keep && pair; pair = pair->next)
      {
        for (field_t *f = field; !keep && f; f = f->next)
        {
          for (node_t *n = f->keys; !keep && n; n = n->next)
            keep = (n->num == pair->key);
        }
      }
    }

    // if where clauses supplied, only retrieve records matching them
    if (keep && where)
    {
      keep = 0;
      for (pair_t *pair = record->pairs; pair; pair = pair->next)
      {
        int cmp = 0;
        for (size_t i = 0; i < where; i++)
        {
          if (
            (wheres[i].flags & W_EQ  && pair->key == wheres[i].key && pair->val == wheres[i].val) ||
            (wheres[i].flags & W_NE  && pair->key == wheres[i].key && pair->val != wheres[i].val) ||
            (wheres[i].flags & W_LT  && pair->key == wheres[i].key && pair->val <  wheres[i].val) ||
            (wheres[i].flags & W_GT  && pair->key == wheres[i].key && pair->val >  wheres[i].val) ||
            (wheres[i].flags & W_AND && pair->key == wheres[i].key && pair->val &  wheres[i].val) ||
            (wheres[i].flags & W_OR  && pair->key == wheres[i].key && pair->val |  wheres[i].val) ||
            (wheres[i].flags & W_XOR && pair->key == wheres[i].key && pair->val ^  wheres[i].val)
          )
            cmp++;
        }
        keep = cmp == where;
      }
    }

    if (keep)
    {
      record_t *row = allocate(sizeof(record_t));
      if (!row) goto fail;

      row->id    = record->id;
      row->pairs = NULL;
      row->next  = NULL;
      row->link  = NULL;

      for (pair_t *pair = record->pairs; pair; pair = pair->next)
      {
        keep = 1;
        if (field)
        {
          keep = 0;
          for (field_t *f = field; !keep && f; f = f->next)
          {
            for (node_t *n = f->keys; !keep && n; n = n->next)
              keep = (n->num == pair->key);
          }
        }
        if (keep)
        {
          pair_t *col = allocate(sizeof(pair_t));
          if (!col) goto fail;

          col->key    = pair->key;
          col->val    = pair->val;
          col->next   = row->pairs;
          row->pairs  = col;
        }
      }

      if (row->pairs)
      {
        if (!first)
          first = row;

        row->link = result;
        if (result)
          result->next = row;
        result = row;
      }
      else
      {
        release(row, sizeof(record_t));
      }
    }
  }
  *results = first;
  return E_OK;

fail:
  release_result(result);
  return rc;
}

int
parse_range (char **src, record_t **results, char *errmsg, field_t **fields)
{
  pthread_rwlock_rdlock(&rwlock);

  char *line = *src;
  int fill_gaps = 0;

  int rc = E_PARSE;
  errmsg[0] = 0;

  number_t id, range, step;

  field_t *field = NULL;

  where_t wheres[MAX_WHERES];
  size_t where = 0;

  record_t *result = NULL, *tmp = NULL;

  // [key[ ... keyN]] [key=val[ ... keyN=val]] [start:stop[:step]]

  while (*line)
  {
    line = strskip(line, isspace);

    int is_field = regmatch(&re_field, line);
    int is_field_aggr = !is_field && regmatch(&re_field_aggr, line);

    if (is_field)
    {
      field_t *f = allocate(sizeof(field_t));

      field_t **prev = &field;
      while (*prev) prev = &(*prev)->next;
      *prev = f; f->next = NULL;

      f->keys = NULL;
      f->flags = 0;
      memset(f->alias, 0, ALIAS);

      node_t *node = allocate(sizeof(node_t));
      node->next = f->keys;
      f->keys = node;

      if (!parse_number(&line, &node->num, f->alias))
        goto key_fail;

      line = strskip(line, isspace);

      if (regmatch(&re_field_as, line))
      {
        line += 3;

        char *alias = f->alias;
        int len = 0;

        for (;
          *line && isalias(*line) && len < ALIAS-1;
          alias[len++] = *line++
        );

        alias[len] = 0;
      }

      continue;
    }

    if (is_field_aggr)
    {
      field_t *f = allocate(sizeof(field_t));

      field_t **prev = &field;
      while (*prev) prev = &(*prev)->next;
      *prev = f; f->next = NULL;

      f->keys = NULL;
      f->flags = 0;
      memset(f->alias, 0, ALIAS);

      fill_gaps = 1;

      if (!strncmp(line, "sum(", 4))
      {
        f->flags = F_SUM;
        line += 4;
      }
      else
      if (!strncmp(line, "diff(", 5))
      {
        f->flags = F_DIFF;
        line += 5;
      }
      else
      if (!strncmp(line, "max(", 4))
      {
        f->flags = F_MAX;
        line += 4;
      }
      else
      if (!strncmp(line, "min(", 4))
      {
        f->flags = F_MIN;
        line += 4;
      }
      else
      if (!strncmp(line, "first(", 6))
      {
        f->flags = F_FIRST;
        line += 6;
      }
      else
      if (!strncmp(line, "last(", 5))
      {
        f->flags = F_LAST;
        line += 5;
      }
      else
      if (!strncmp(line, "mean(", 5))
      {
        f->flags = F_MEAN;
        line += 5;
      }
      else
      if (!strncmp(line, "median(", 7))
      {
        f->flags = F_MEDIAN;
        line += 7;
      }

      for (int i = 0; ; i++)
      {
        number_t num;

        if (*line == ')' || !parse_number(&line, &num, f->alias))
        {
          if (i == 0)
            goto key_fail;
          break;
        }

        node_t *node = allocate(sizeof(node_t));

        node_t **prev = &f->keys;
        while (*prev) prev = &(*prev)->next;
        *prev = node;
        node->next = NULL;

        node->num = num;

        if (*line == ',') line++;
      }

      line++;
      line = strskip(line, isspace);

      if (regmatch(&re_field_as, line))
      {
        line += 3;

        char *alias = f->alias;
        int len = 0;

        for (;
          *line && isalias(*line) && len < ALIAS-1;
          alias[len++] = *line++
        );

        alias[len] = 0;
      }

      continue;
    }

    if (regmatch(&re_where, line))
    {
      if (where == MAX_WHERES)
      {
        snprintf(errmsg, LINE, "exceeded %d filters", MAX_WHERES);
        goto fail;
      }

      wheres[where].flags = 0;

      if (!parse_number(&line, &wheres[where].key, NULL))
        goto key_fail;

      switch (*line++)
      {
        case '=': wheres[where].flags = W_EQ;
        case '!': wheres[where].flags = W_NE;
        case '<': wheres[where].flags = W_LT;
        case '>': wheres[where].flags = W_GT;
        case '&': wheres[where].flags = W_AND;
        case '|': wheres[where].flags = W_OR;
        case '^': wheres[where].flags = W_XOR;
      }

      if (!parse_number(&line, &wheres[where].val, NULL))
        goto val_fail;

      where++;
      continue;
    }

    if (regmatch(&re_range, line))
    {
      if (!parse_number(&line, &id, NULL))
        goto key_fail;

      line++; // :

      if (!parse_number(&line, &range, NULL))
        goto key_fail;

      step = 1;

      if (*line == ':')
      {
        line++; // :

        if (!parse_number(&line, &step, NULL))
          goto num_fail;
      }

      rc = fetch_range(&result, id, range, fill_gaps ? 1: step, field, wheres, where);

      if (rc != E_OK)
        goto fail;

      if (fill_gaps)
      {
        record_t *first = NULL, *consume = result;
        tmp = result; result = NULL;

        for (number_t i = id; i <= range; i += step)
        {
          record_t *row = allocate(sizeof(record_t));
          if (!row) { rc = E_SERVER; goto fail; }

          row->id    = i;
          row->pairs = NULL;
          row->next  = NULL;
          row->link  = NULL;

          int n = 0;
          for (field_t *f = field; f; f = f->next, n++)
          {
            pair_t *col = allocate(sizeof(pair_t));
            if (!col) { rc = E_SERVER; goto fail; }

            pair_t **prev = &row->pairs;
            while (*prev) prev = &(*prev)->next;
            *prev = col;
            col->next = NULL;

            col->key = f->keys->num;
            col->val = 0;

            f->count_nodes = 0;
            f->count_rows  = 0;

            f->sum   = 0;
            f->min   = 0;
            f->max   = 0;
            f->diff1 = 0;
            f->diff2 = 0;
            f->col   = col;
          }

          for (number_t j = i; j <= range && j < i+step; j++)
          {
            if (consume && consume->id == j)
            {
              for (field_t *f = field; f; f = f->next)
              {
                pair_t *col = f->col;
                int touched = 0;

                for (node_t *node = f->keys; node; node = node->next)
                {
                  for (pair_t *pair = consume->pairs; pair; pair = pair->next)
                  {
                    if (node->num == pair->key)
                    {
                      if (f->flags == F_LAST)
                        col->val = pair->val;

                      else if (f->count_nodes == 0) // implicit first()
                        col->val = pair->val;

                      f->sum  += pair->val;
                      f->min   = f->count_nodes == 0 || pair->val < f->min ? pair->val: f->min;
                      f->max   = f->max > pair->val ? f->max: pair->val;

                      f->diff1 += f->keys == node ? pair->val: 0;
                      f->diff2 += f->keys == node ? 0: pair->val;

                      f->count_nodes++;
                      touched = 1;
                    }
                  }
                }
                if (touched)
                  f->count_rows++;
              }
              consume = consume->next;
            }
          }

          for (field_t *f = field; f; f = f->next)
          {
            pair_t *col = f->col;

            if (f->flags == F_MEAN)
              col->val = f->count_nodes ? f->sum / f->count_nodes: 0;

            else if (f->flags == F_MEDIAN)
              col->val = f->max - ((f->max - f->min) /2);

            else if (f->flags == F_MIN)
              col->val = f->min;

            else if (f->flags == F_MAX)
              col->val = f->max;

            else if (f->flags == F_SUM)
              col->val = f->sum;

            else if (f->flags == F_DIFF)
            {
              col->val = f->diff1 > f->diff2 ? (f->diff1 - f->diff2) : 0;
              col->val = col->val / (f->count_rows ? f->count_rows: 1);
            }
          }

          if (!first)
            first = row;

          row->link = result;
          if (result)
            result->next = row;
          result = row;
        }

        release_result(tmp);
        tmp = NULL;
        *results = first;
        goto done;
      }

      *results = result;
      goto done;
    }

    snprintf(errmsg, LINE, "unknown syntax at: %s", line);
    goto fail;
  }

done:
  if (fields) *fields = field; else release_fields(field);
  pthread_rwlock_unlock(&rwlock);
  *src = line;
  return E_OK;

key_fail:
  snprintf(errmsg, LINE, "expected key at: %s", line);
  goto fail;

val_fail:
  snprintf(errmsg, LINE, "expected val at: %s", line);
  goto fail;

num_fail:
  snprintf(errmsg, LINE, "expected num at: %s", line);
  goto fail;

fail:
  pthread_rwlock_unlock(&rwlock);
  release_result(tmp);
  release_result(result);
  release_fields(field);
  return rc;
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

void
parse_select (char *line)
{
  record_t *result = NULL;
  char errmsg[LINE];

  field_t *fields = NULL;

  int rc = parse_range(&line, &result, errmsg, &fields);

  if (rc == E_OK)
  {
    size_t results = 0;
    for (record_t *row = result;
      row;
      results++, row = row->next
    );

    respondf("%u %lu\n", E_OK, results);

    for (record_t *row = result; row; row = row->next)
    {
      respondf("%lu", row->id);

      int n = 0;
      for (pair_t *col = row->pairs; col; col = col->next, n++)
      {
        int found = 0, fn = 0;
        for (field_t *f = fields; !found && f; f = f->next, fn++)
        {
          if (n == fn && f->alias[0])
          {
            respondf(" %s %lu", f->alias, col->val);
            found = 1;
          }
        }
        if (!found)
        {
          respondf(" %lu %lu", col->key, col->val);
        }
      }
      respondf("\n");
    }

    release_result(result);
    release_fields(fields);
    return;
  }

  respondf("%u %s\n", rc, errmsg);
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
