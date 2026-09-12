# HDFS Command Cheat Sheet

All commands use the prefix `hdfs dfs` (the modern form) — the older `hadoop fs` still works identically.

## File & Directory Basics

| Task | Command |
|---|---|
| List directory contents | `hdfs dfs -ls /path` |
| List recursively | `hdfs dfs -ls -R /path` |
| Make a directory | `hdfs dfs -mkdir /path` |
| Make directory + parents | `hdfs dfs -mkdir -p /path/to/dir` |
| Remove a file | `hdfs dfs -rm /path/file` |
| Remove a directory (recursive) | `hdfs dfs -rm -r /path` |
| Skip trash on delete | `hdfs dfs -rm -skipTrash /path/file` |
| Remove empty directory | `hdfs dfs -rmdir /path` |

## Copying Data

| Task | Command |
|---|---|
| Copy from local → HDFS | `hdfs dfs -put localfile /hdfs/path` |
| Copy from local → HDFS (alt) | `hdfs dfs -copyFromLocal localfile /hdfs/path` |
| Copy from HDFS → local | `hdfs dfs -get /hdfs/path localfile` |
| Copy from HDFS → local (alt) | `hdfs dfs -copyToLocal /hdfs/path localfile` |
| Copy within HDFS | `hdfs dfs -cp /src/path /dst/path` |
| Move within HDFS | `hdfs dfs -mv /src/path /dst/path` |
| Merge files → single local file | `hdfs dfs -getmerge /hdfs/dir localfile.txt` |

## Viewing File Content

| Task | Command |
|---|---|
| Print file contents | `hdfs dfs -cat /path/file` |
| View last KB of a file | `hdfs dfs -tail /path/file` |
| View first few lines (via pipe) | `hdfs dfs -cat /path/file \| head` |
| View compressed text files | `hdfs dfs -text /path/file` |

## File Info & Sizes

| Task | Command |
|---|---|
| Disk usage of a dir/file | `hdfs dfs -du /path` |
| Human-readable sizes | `hdfs dfs -du -h /path` |
| Total summarized size | `hdfs dfs -du -s -h /path` |
| Total cluster usage | `hdfs dfs -df -h` |
| Count files/dirs/bytes | `hdfs dfs -count /path` |
| Count (human-readable) | `hdfs dfs -count -h /path` |
| Check file exists / type | `hdfs dfs -test -e /path/file` |

## Permissions & Ownership

| Task | Command |
|---|---|
| Change permissions | `hdfs dfs -chmod 755 /path` |
| Change recursively | `hdfs dfs -chmod -R 755 /path` |
| Change owner | `hdfs dfs -chown user:group /path` |
| Change owner recursively | `hdfs dfs -chown -R user:group /path` |
| Change group | `hdfs dfs -chgrp group /path` |

## Replication & Blocks

| Task | Command |
|---|---|
| Set replication factor | `hdfs dfs -setrep -w 3 /path/file` |
| Check block locations | `hdfs fsck /path/file -files -blocks -locations` |

## Admin & Cluster Health (run as HDFS superuser)

| Task | Command |
|---|---|
| Filesystem check | `hdfs fsck /` |
| Safe mode status | `hdfs dfsadmin -safemode get` |
| Enter safe mode | `hdfs dfsadmin -safemode enter` |
| Leave safe mode | `hdfs dfsadmin -safemode leave` |
| Cluster report | `hdfs dfsadmin -report` |
| Refresh nodes | `hdfs dfsadmin -refreshNodes` |
| Balance the cluster | `hdfs balancer` |
| List datanode info | `hdfs dfsadmin -printTopology` |

## Trash

| Task | Command |
|---|---|
| Empty trash now | `hdfs dfs -expunge` |
| List trash contents | `hdfs dfs -ls /user/$USER/.Trash` |

## Quick Tips
- Add `-h` to most size-related commands (`-du`, `-count`) for human-readable output (KB/MB/GB).
- Add `-R` to `-ls`, `-chmod`, `-chown` for recursive operations.
- `hdfs dfs -help <command>` shows detailed usage for any subcommand.
- Wildcards work: `hdfs dfs -ls /data/*.csv`
