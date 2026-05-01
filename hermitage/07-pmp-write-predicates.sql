drop table if exists test;
create table test (id int primary key, value int);
insert into test (id, value) values (1, 10), (2, 20);

---

t1: begin;
t1: $SHOW_ISOLATION;
t2: begin;
t2: $SHOW_ISOLATION;

t1: update test set value = value + 10;
t2: delete from test where value = 20;             -- group final; assert ok or t2aborted => error
t1: commit;
t2: select * from test where value = 20;           -- group final; assert error
t2: commit;                                         -- group final
