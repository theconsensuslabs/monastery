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
t3: select * from test where id = 1; -- assert ({1, 11}) or ({1, 12})
t2: update test set value = 18 where id = 2;
t3: select * from test where id = 2; -- assert ({2, 19}) or ({2, 18})
t2: commit;
t3: select * from test where id = 2; -- group t3view; assert fresh => ({2, 18}) or stale => ({2, 19})
t3: select * from test where id = 1; -- group t3view; assert fresh => ({1, 12}) or stale => ({1, 11})
t3: commit;
