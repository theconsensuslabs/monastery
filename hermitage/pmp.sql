drop table if exists test;
create table test (id int primary key, value int);
insert into test (id, value) values (1, 10), (2, 20);

---

t1: begin
t2: begin
t1: select * from test where value = 30;
t2: insert into test (id, value) values(3, 30);
t2: commit;
t1: select * from test where value % 3 = 0;
t1: commit;
