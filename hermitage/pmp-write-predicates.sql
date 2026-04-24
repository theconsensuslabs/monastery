t0: drop table if exists test;
t0: create table test (id int primary key, value int);
t0: insert into test (id, value) values (1, 10), (2, 20);

t1: begin
t2: begin

t1: update test set value = value + 10;
t2: delete from test where value = 20;
t1: commit;
t2: select * from test where value = 20; -- Should fail in repeatable-read+.
t2: commit;
