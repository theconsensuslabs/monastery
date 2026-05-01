drop table if exists test;
create table test (id int primary key, value int);
insert into test (id, value) values (1, 10), (2, 20);

---

t1: begin;
t1: $SHOW_ISOLATION
t1: select * from test;

t2: begin;
t2: $SHOW_ISOLATION
t2: update test set value = value + 5 where id = 2;
t2: commit;

t3: begin;
t3: $SHOW_ISOLATION
t3: select * from test;

t3: commit;
t1: update test set value = 0 where id = 1; -- assert error
t1: abort;
