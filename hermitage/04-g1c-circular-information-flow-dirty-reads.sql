drop table if exists test;
create table test (id int primary key, value int);
insert into test (id, value) values (1, 10), (2, 20);

---

t1: begin;
t1: $SHOW_ISOLATION;
t2: begin;
t2: $SHOW_ISOLATION;

t1: update test set value = 11 where id = 1;
t2: update test set value = 22 where id = 2;
t1: select * from test where id = 2; -- group g1c; assert t1first => ({2, 20}) or t2first => ({2, 22}) or snapshot => ({2, 20}) or error
t2: select * from test where id = 1; -- group g1c; assert t1first => ({1, 11}) or t2first => ({1, 10}) or snapshot => ({1, 10}) or error
t1: commit;
t2: commit;
