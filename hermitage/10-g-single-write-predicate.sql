drop table if exists test;
create table test (id int primary key, value int);
insert into test (id, value) values (1, 10), (2, 20);

---

t1: begin;
t1: $SHOW_ISOLATION;
t2: begin;
t2: $SHOW_ISOLATION;

t1: select * from test where id = 1;
t2: select * from test;
t2: update test set value = 12 where id = 1;
t2: update test set value = 18 where id = 2;
t2: commit;

t1: delete from test where value = 20; -- assert error
t1: rollback;
