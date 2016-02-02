# n3

A nested map of unsigned 64-bit integers.

    [ ID => { key: value } ]

    map <uint64_t, map <uint64_t, uint64_t>>

* No restriction on what each number means:
    * IDs might be UNIX timestamps / keys might be metrics / values might be scaled fixed-point measurements
    * IDs might be object identifiers / keys might be references to other objects / values might be weights
    * IDs might be RDBMS surrogate keys / keys might be table fields / values ... etc
* Numbers can have text aliases for query readbility
* IDs are sorted in ascending order

## Example: Time Series

    alias 1 cpu
    alias 2 memory
    alias 3 disk

    insert 1454376744 cpu 39 memory 765504 disk 6245996
    insert 1454376864 cpu 95 memory 781636 disk 6246356
    insert 1454376984 cpu 51 memory 777996 disk 6246756
    insert 1454377104 cpu 33 memory 794044 disk 6247084
    insert 1454377224 cpu 34 memory 797196 disk 6247464
    insert 1454377344 cpu 38 memory 808208 disk 6247880
    insert 1454377464 cpu 33 memory 825500 disk 6248244
    insert 1454377584 cpu 98 memory 828884 disk 6248616
    insert 1454377704 cpu 42 memory 829188 disk 6248936
    insert 1454377824 cpu 27 memory 839720 disk 6249332

    select mean(cpu) as cpu_pct max(memory) as mem_kb max(disk) as disk_kb from 1454376744:1454377824:300

    0 4
    1454376744 cpu_pct 61 mem_kb 781636 disk_kb 6246756
    1454377044 cpu_pct 33 mem_kb 797196 disk_kb 6247464
    1454377344 cpu_pct 56 mem_kb 828884 disk_kb 6248616
    1454377644 cpu_pct 34 mem_kb 839720 disk_kb 6249332

## Query Syntax

### Insert

    insert id key val [ ... keyN valN]

### Delete keys (preserving IDs) by range

    delete key ... keyN from id:idN

### Delete IDs and keys

    delete id:idN

### Select keys by range

    select key [ ... keyN ] from id:idN

### Select keys by range with aggregation

    select func(key) [ ... funcN(keyN) ] from id:idN:step

### Select keys by range with aggregation, no gaps

    select func(key) [ ... funcN(keyN) ] from id:idN:step fill

## Supported Functions

* min
* max
* mean
* median
* first
* last
* sum
* diff

