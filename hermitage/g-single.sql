t0: drop table if exists test;
t0: create table test (id int primary key, value int);
t0: insert into test (id, value) values (1, 10), (2, 20);

t1: begin;
t2: begin;

t1: select * from test where id = 1; -- T1. Shows 1 => 10
t2: select * from test where id = 1; -- T2
t2: select * from test where id = 2; -- T2

t2: update test set value = 12 where id = 1; -- T2
t2: update test set value = 18 where id = 2; -- T2
t2: commit; -- T2

t1: select * from test where id = 2; -- T1. Shows 2 => 20
t1: commit; -- T1
