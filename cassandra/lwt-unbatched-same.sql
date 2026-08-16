# Transfers as two independent lightweight transactions, one per row.
#
# Accounts (1,1) and (1,2) share the customer 1 partition.
#
# Each write is a conditional UPDATE, so each row is compare-and-set on
# its own and no update is lost. Nothing groups the two, so a transfer
# whose first leg applies and whose second does not leaves half of
# itself behind, and there is nothing to undo it.
#
# This is the cell the other scripts leave empty: a condition without a
# batch. lwt-same.sql is the same workload with the two writes moved
# into one conditional batch.

create keyspace if not exists lab with replication = {'class':'NetworkTopologyStrategy','datacenter1':3};
drop table if exists lab.accounts;
create table lab.accounts (customer int, account_id int, balance int, op1 int, op2 int, primary key (customer, account_id));
insert into lab.accounts (customer, account_id, balance, op1, op2) values (1, 1, 1000, 0, 0);
insert into lab.accounts (customer, account_id, balance, op1, op2) values (1, 2, 1000, 0, 0);

--- concurrent

w1: repeat 200 {
  a, p = select balance, balance - 1 from lab.accounts where customer = 1 and account_id = 1;
  b, q = select balance, balance + 1 from lab.accounts where customer = 1 and account_id = 2;
  update lab.accounts set balance = {p} \
    where customer = 1 and account_id = 1 if balance = {a};  -- assert ok or error
  update lab.accounts set balance = {q} \
    where customer = 1 and account_id = 2 if balance = {b};  -- assert ok or error
}

w2: repeat 200 {
  a, p = select balance, balance - 1 from lab.accounts where customer = 1 and account_id = 1;
  b, q = select balance, balance + 1 from lab.accounts where customer = 1 and account_id = 2;
  update lab.accounts set balance = {p} \
    where customer = 1 and account_id = 1 if balance = {a};  -- assert ok or error
  update lab.accounts set balance = {q} \
    where customer = 1 and account_id = 2 if balance = {b};  -- assert ok or error
}

r1: consistency serial;
r1: repeat 1000 {
  select balance from lab.accounts where customer = 1; -- assert sum(0) = 2000
}


---

check: select balance from lab.accounts where customer = 1; -- assert sum(0) = 2000
