t0: drop table if exists test;
t0: create table test (id int primary key, value int);
t0: insert into test (id, value) values (1, 10), (2, 20);

t1: begin;
t2: begin;

t1: select * from test where id = 1;
t2: select * from test where id = 1;

t1: update test set value = 11 where id = 1;
t2: update test set value = 11 where id = 1; -- Should fail in repeatable-read+.

t1: commit;
t2: commit;
