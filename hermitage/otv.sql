drop table if exists test;
create table test (id int primary key, value int);
insert into test (id, value) values (1, 10), (2, 20);

---

t1: begin;
t1: $SHOW_ISOLATION
t2: begin;
t2: $SHOW_ISOLATION
t3: begin;
t3: $SHOW_ISOLATION

t1: update test set value = 11 where id = 1;
t1: update test set value = 19 where id = 2;
t2: update test set value = 12 where id = 1;
t1: commit;
t3: select * from test where id = 1; -- Read-committed+ should still show 1 => 11
t2: update test set value = 18 where id = 2;
t3: select * from test where id = 2; -- Read-committed+ should still show 2 => 19
t2: commit;
t3: select * from test where id = 2; -- Read-committed+ should still show 2 => 18
t3: select * from test where id = 1; -- Read-committed+ should still show 1 => 12
t3: commit;
