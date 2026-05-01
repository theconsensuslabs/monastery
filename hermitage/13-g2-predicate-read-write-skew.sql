drop table if exists test;
create table test (id int primary key, value int);
insert into test (id, value) values (1, 10), (2, 20);

---

t1: begin;
t1: $SHOW_ISOLATION
t2: begin;
t2: $SHOW_ISOLATION

t1: select * from test where value % 3 = 0;
t2: select * from test where value % 3 = 0;
t1: insert into test (id, value) values(3, 30);
t2: insert into test (id, value) values(4, 42); 
t1: commit;
t2: commit; -- assert error
