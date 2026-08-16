# Transfers as a conditional BATCH, a lightweight transaction.
#
# Accounts (1,1) and (1,2) share the customer 1 partition, and Paxos
# runs per partition.
#
# The IF conditions carry the balances the writer read, so a transfer that
# raced comes back [applied] = false and the retry block re-proposes.
#
# A CAS that times out reports "CAS operation result is unknown" whether or
# not it was accepted. Each writer stamps its transfer number into its own
# sequence column, and because `op1 < {op}` admits a transfer only once,
# a replay does nothing. The step after the batch reads the column back, ending the block
# when the transfer is recorded rather than when a response arrived.
#
# The reader reads at SERIAL, which completes any Paxos round a QUORUM read
# could otherwise catch mid-flight.

create keyspace if not exists lab with replication = {'class':'NetworkTopologyStrategy','datacenter1':3};
drop table if exists lab.accounts;
create table lab.accounts (customer int, account_id int, balance int, op1 int, op2 int, primary key (customer, account_id));
insert into lab.accounts (customer, account_id, balance, op1, op2) values (1, 1, 1000, 0, 0);
insert into lab.accounts (customer, account_id, balance, op1, op2) values (1, 2, 1000, 0, 0);

--- concurrent

w1: repeat 200 as op {
  retry 50 {
    a, p = select balance, balance - 1 from lab.accounts where customer = 1 and account_id = 1;
    b, q = select balance, balance + 1 from lab.accounts where customer = 1 and account_id = 2;
    begin batch update lab.accounts set balance = {p}, op1 = {op} where customer = 1 and account_id = 1 if balance = {a} and op1 < {op}; update lab.accounts set balance = {q} where customer = 1 and account_id = 2 if balance = {b}; apply batch; -- assert ok or error
    select op1 from lab.accounts where customer = 1 and account_id = 1; -- assert ({{op}})
  }
}

w2: repeat 200 as op {
  retry 50 {
    a, p = select balance, balance - 1 from lab.accounts where customer = 1 and account_id = 1;
    b, q = select balance, balance + 1 from lab.accounts where customer = 1 and account_id = 2;
    begin batch update lab.accounts set balance = {p}, op2 = {op} where customer = 1 and account_id = 1 if balance = {a} and op2 < {op}; update lab.accounts set balance = {q} where customer = 1 and account_id = 2 if balance = {b}; apply batch; -- assert ok or error
    select op2 from lab.accounts where customer = 1 and account_id = 1; -- assert ({{op}})
  }
}

r1: consistency serial;
r1: repeat 1000 {
  select balance from lab.accounts where customer = 1; -- assert sum(0) = 2000
}

---

# atomicity: the pair holds 2000 units at every instant.
check: select balance from lab.accounts where customer = 1; -- assert sum(0) = 2000

# no lost updates: 400 transfers of 1 unit out of the payer.
check: select balance from lab.accounts where customer = 1 and account_id = 1; -- assert ({600})
check: select balance from lab.accounts where customer = 1 and account_id = 2; -- assert ({1400})
