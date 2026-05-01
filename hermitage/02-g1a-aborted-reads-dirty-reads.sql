drop table if exists test;
create table test (id int primary key, value int);
insert into test (id, value) values (1, 10), (2, 20);

---

t1: begin;
t1: $SHOW_ISOLATION;
t2: begin;
t2: $SHOW_ISOLATION;

t1: update test set value = 101 where id = 1;
t2: select * from test; -- assert [{1, 10}, {2, 20}]
t1: abort;
t2: select * from test; -- assert [{1, 10}, {2, 20}]
t2: commit;
