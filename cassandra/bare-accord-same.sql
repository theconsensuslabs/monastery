# Transfers as bare writes in an Accord transaction.
#
# Accounts (1,1) and (1,2) share the customer 1 partition.
#
# The transaction needs no LET and no IF, because a bare write has
# nothing to read and nothing to decide. The reader reads inside a
# transaction and projects both balances, since Accord forbids
# aggregates inside one.

create keyspace if not exists lab with replication = {'class':'NetworkTopologyStrategy','datacenter1':3};
drop table if exists lab.accounts;
create table lab.accounts (customer int, account_id int, balance int, primary key (customer, account_id)) with transactional_mode = 'full';
insert into lab.accounts (customer, account_id, balance) values (1, 1, 1000);
insert into lab.accounts (customer, account_id, balance) values (1, 2, 1000);

--- concurrent

w1: repeat 400 as x {
  begin transaction update lab.accounts set balance = {x} where customer = 1 and account_id = 1; update lab.accounts set balance = 2000 - {x} where customer = 1 and account_id = 2; commit transaction; -- assert ok
}

w2: repeat 400 as x {
  begin transaction update lab.accounts set balance = 2000 - {x} where customer = 1 and account_id = 1; update lab.accounts set balance = {x} where customer = 1 and account_id = 2; commit transaction; -- assert ok
}

r1: repeat 1500 {
  begin transaction let x = (select balance from lab.accounts where customer = 1 and account_id = 1); let y = (select balance from lab.accounts where customer = 1 and account_id = 2); select x.balance, y.balance; commit transaction; -- assert sum(0, 1) = 2000
}

---

# atomicity and isolation: the pair totals 2000 at every instant.
check: select balance from lab.accounts where customer = 1; -- assert sum(0) = 2000
