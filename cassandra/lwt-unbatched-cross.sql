# Transfers as two independent lightweight transactions, one per row.
#
# customer is the partition key, so (1,1) and (2,1) are in different
# partitions. Each write is still single-partition, so unlike
# lwt-cross.sql these statements are accepted and do apply.
#
# Each write is a conditional UPDATE, so each row is compare-and-set on
# its own and no update is lost. Nothing groups the two, so a transfer
# whose first leg applies and whose second does not leaves half of
# itself behind, and there is nothing to undo it.
#
# This is the cell the other scripts leave empty: a condition without a
# batch. lwt-cross.sql is the same workload with the two writes moved
# into one conditional batch.
#
# A read spanning two partitions has no atomic form here, so a torn read
# may be the reader's doing. The check after the run is the result to
# trust: it runs alone, and a total other than 2000 is durable.

create keyspace if not exists lab with replication = {'class':'NetworkTopologyStrategy','datacenter1':3};
drop table if exists lab.accounts;
create table lab.accounts (customer int, account_id int, balance int, op1 int, op2 int, primary key (customer, account_id));
insert into lab.accounts (customer, account_id, balance, op1, op2) values (1, 1, 1000, 0, 0);
insert into lab.accounts (customer, account_id, balance, op1, op2) values (2, 1, 1000, 0, 0);

--- concurrent

w1: repeat 200 {
  a, p = select balance, balance - 1 from lab.accounts where customer = 1 and account_id = 1;
  b, q = select balance, balance + 1 from lab.accounts where customer = 2 and account_id = 1;
  update lab.accounts set balance = {p} \
    where customer = 1 and account_id = 1 if balance = {a};  -- assert ok or error
  update lab.accounts set balance = {q} \
    where customer = 2 and account_id = 1 if balance = {b};  -- assert ok or error
}

w2: repeat 200 {
  a, p = select balance, balance - 1 from lab.accounts where customer = 1 and account_id = 1;
  b, q = select balance, balance + 1 from lab.accounts where customer = 2 and account_id = 1;
  update lab.accounts set balance = {p} \
    where customer = 1 and account_id = 1 if balance = {a};  -- assert ok or error
  update lab.accounts set balance = {q} \
    where customer = 2 and account_id = 1 if balance = {b};  -- assert ok or error
}

r1: repeat 1000 {
  select balance from lab.accounts where customer in (1, 2); -- assert sum(0) = 2000
}


---

check: select balance from lab.accounts where customer in (1, 2); -- assert sum(0) = 2000
