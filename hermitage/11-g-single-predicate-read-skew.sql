drop table if exists test;
create table test (id int primary key, value int);
insert into test (id, value) values (1, 10), (2, 20);

---

t1: begin;
t1: $SHOW_ISOLATION;
t2: begin;
t2: $SHOW_ISOLATION;

t1: select * from test where value % 5 = 0;
t2: update test set value = 12 where value = 10;
t2: commit;

t1: select * from test where value % 3 = 0; -- assert ()
t1: commit;
