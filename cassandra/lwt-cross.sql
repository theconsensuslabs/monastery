# Transfers as a conditional BATCH across a partition boundary.
#
# customer is the partition key, so (1,1) and (2,1) are in different
# partitions. Paxos runs per partition and a conditional batch cannot
# span two, so every write here is rejected and the balances never move.

create keyspace if not exists lab with replication = {'class':'NetworkTopologyStrategy','datacenter1':3};
drop table if exists lab.accounts;
create table lab.accounts (customer int, account_id int, balance int, op1 int, op2 int, primary key (customer, account_id));
insert into lab.accounts (customer, account_id, balance, op1, op2) values (1, 1, 1000, 0, 0);
insert into lab.accounts (customer, account_id, balance, op1, op2) values (2, 1, 1000, 0, 0);

--- concurrent

w1: repeat 200 {
  a, p = select balance, balance - 1 from lab.accounts where customer = 1 and account_id = 1;
  b, q = select balance, balance + 1 from lab.accounts where customer = 2 and account_id = 1;
  begin batch update lab.accounts set balance = {p} where customer = 1 and account_id = 1 if balance = {a}; update lab.accounts set balance = {q} where customer = 2 and account_id = 1 if balance = {b}; apply batch; -- assert error
}

w2: repeat 200 {
  a, p = select balance, balance - 1 from lab.accounts where customer = 1 and account_id = 1;
  b, q = select balance, balance + 1 from lab.accounts where customer = 2 and account_id = 1;
  begin batch update lab.accounts set balance = {p} where customer = 1 and account_id = 1 if balance = {a}; update lab.accounts set balance = {q} where customer = 2 and account_id = 1 if balance = {b}; apply batch; -- assert error
}

r1: repeat 1000 {
  select balance from lab.accounts where customer in (1, 2); -- assert sum(0) = 2000
}

---

# atomicity: the pair holds 2000 units at every instant.
check: select balance from lab.accounts where customer in (1, 2); -- assert sum(0) = 2000

# nothing applied, so the balances are untouched.
check: select balance from lab.accounts where customer = 1 and account_id = 1; -- assert ({1000})
check: select balance from lab.accounts where customer = 2 and account_id = 1; -- assert ({1000})
