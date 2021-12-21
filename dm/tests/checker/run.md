# manual test
1. run create_tables.sql in source
2. run dm-master, dm-worker1, dm-worker2
3. create source
4. check-task, calculate spend time
5. change table numbers in create_tables.sql and network delay in source

# test results

## 4 threads 10000 tables
|test mechine|added delay|spend time|
| ------------|-----------|---------|
|local|0 ms|3.9 s|
|125|0 ms|8.5 s|
|125|10 ms|31.7 s|
|106|0 ms|11.2 s|
|106|10 ms|29.6 s|
|106|20 ms|54.5 s|
|106|50 ms|128.5 s|
|106|100 ms|253 s|
