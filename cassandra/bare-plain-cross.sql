# Transfers as bare writes, sent as two ordinary UPDATEs.
#
# customer is the partition key, so (1,1) and (2,1) are in
# different partitions.
#
# Nothing is read. Each writer sends a self-consistent pair that totals
# 2000, taking the amount from the loop variable and letting CQL compute
# the complement. With no read to go stale there are no lost updates, so
# a total other than 2000 came from the write alone.

create keyspace if not exists lab with replication = {'class':'NetworkTopologyStrategy','datacenter1':3};
drop table if exists lab.accounts;
create table lab.accounts (customer int, account_id int, balance int, primary key (customer, account_id));
insert into lab.accounts (customer, account_id, balance) values (1, 1, 1000);
insert into lab.accounts (customer, account_id, balance) values (2, 1, 1000);

--- concurrent

w1: repeat 400 as x {
  update lab.accounts set balance = {x} where customer = 1 and account_id = 1;
  update lab.accounts set balance = 2000 - {x} where customer = 2 and account_id = 1;
}

w2: repeat 400 as x {
  update lab.accounts set balance = 2000 - {x} where customer = 1 and account_id = 1;
  update lab.accounts set balance = {x} where customer = 2 and account_id = 1;
}

r1: repeat 1500 {
  select balance from lab.accounts where customer in (1, 2); -- assert sum(0) = 2000
}

---

# atomicity and isolation: the pair totals 2000 at every instant.
check: select balance from lab.accounts where customer in (1, 2); -- assert sum(0) = 2000
