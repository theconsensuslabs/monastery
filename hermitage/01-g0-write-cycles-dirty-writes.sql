drop table if exists test;
create table test (id int primary key, value int);
insert into test (id, value) values (1, 10), (2, 20);

---

t1: begin;
t1: $SHOW_ISOLATION;
t2: begin;
t2: $SHOW_ISOLATION;

t1: update test set value = 11 where id = 1;
t2: update test set value = 12 where id = 1;
t1: update test set value = 21 where id = 2;
t1: commit;
t1: select * from test; -- assert [{1, 11}, {2, 21}]
t2: update test set value = 22 where id = 2;
t2: commit;
t1: select * from test; -- assert [{1, 12}, {2, 22}] or [{1, 11}, {2, 21}]
t2: select * from test; -- assert [{1, 12}, {2, 22}] or [{1, 11}, {2, 21}]
