t0: drop table if exists test;
t0: create table test (id int primary key, value int);
t0: insert into test (id, value) values (1, 10), (2, 20);

t1: begin;
t2: begin;

t1: select * from test where value % 5 = 0; -- T1
t2: update test set value = 12 where value = 10; -- T2
t2: commit; -- T2
t1: select * from test where value % 3 = 0; -- T1. Returns nothing
t1: commit; -- T1
