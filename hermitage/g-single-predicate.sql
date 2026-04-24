drop table if exists test;
create table test (id int primary key, value int);
insert into test (id, value) values (1, 10), (2, 20);

---

t1: begin;
t2: begin;

t1: select * from test where value % 5 = 0;
t2: update test set value = 12 where value = 10;
t2: commit;

t1: select * from test where value % 3 = 0; -- Should return nothing on repeatable-read+.
t1: commit;
