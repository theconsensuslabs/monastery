# Transfers with both writes in a logged BATCH.
#
# Accounts (1,1) and (1,2) share the customer 1 partition.
#
# A logged batch is atomic: if any mutation is applied, all of them
# eventually are. It is not isolated, and it does not cover the two reads
# that computed the values.

create keyspace if not exists lab with replication = {'class':'NetworkTopologyStrategy','datacenter1':3};
drop table if exists lab.accounts;
create table lab.accounts (customer int, account_id int, balance int, op1 int, op2 int, primary key (customer, account_id));
insert into lab.accounts (customer, account_id, balance, op1, op2) values (1, 1, 1000, 0, 0);
insert into lab.accounts (customer, account_id, balance, op1, op2) values (1, 2, 1000, 0, 0);

--- concurrent

w1: repeat 200 {
  p = select balance - 1 from lab.accounts where customer = 1 and account_id = 1;
  q = select balance + 1 from lab.accounts where customer = 1 and account_id = 2;
  begin batch \
    update lab.accounts set balance = {p} where customer = 1 and account_id = 1; \
    update lab.accounts set balance = {q} where customer = 1 and account_id = 2; \
    apply batch;
}

w2: repeat 200 {
  p = select balance - 1 from lab.accounts where customer = 1 and account_id = 1;
  q = select balance + 1 from lab.accounts where customer = 1 and account_id = 2;
  begin batch \
    update lab.accounts set balance = {p} where customer = 1 and account_id = 1; \
    update lab.accounts set balance = {q} where customer = 1 and account_id = 2; \
    apply batch;
}

r1: repeat 1000 {
  select balance from lab.accounts where customer = 1; -- assert sum(0) = 2000
}

---

# atomicity: the pair holds 2000 units at every instant.
check: select balance from lab.accounts where customer = 1; -- assert sum(0) = 2000

# no lost updates: 400 transfers of 1 unit out of the payer.
check: select balance from lab.accounts where customer = 1 and account_id = 1; -- assert ({600})
check: select balance from lab.accounts where customer = 1 and account_id = 2; -- assert ({1400})
