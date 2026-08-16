# Transfers as Accord transactions.
#
# Accounts (1,1) and (1,2) share the customer 1 partition.
#
# The table is transactional_mode = 'full', so every read and write goes
# through Accord. `balance -= 1` is legal, which plain CQL rejects on a
# non-counter column, so the read, the guard and both writes are one round
# trip.
#
# A commit that times out leaves the outcome unknown, the same exposure a
# CAS has. Each writer stamps its transfer number into its own sequence
# column, and because `x.op1 < {op}` admits a transfer only once, a
# replay does nothing.
# The step after the transaction reads the column back, ending the block
# when the transfer is recorded rather than when a response arrived.
#
# Accord forbids aggregates inside a transaction, so the reader projects
# both balances and the harness totals them.

create keyspace if not exists lab with replication = {'class':'NetworkTopologyStrategy','datacenter1':3};
drop table if exists lab.accounts;
create table lab.accounts (customer int, account_id int, balance int, op1 int, op2 int, primary key (customer, account_id)) with transactional_mode = 'full';
insert into lab.accounts (customer, account_id, balance, op1, op2) values (1, 1, 1000, 0, 0);
insert into lab.accounts (customer, account_id, balance, op1, op2) values (1, 2, 1000, 0, 0);

--- concurrent

w1: repeat 200 as op {
  retry 50 {
    begin transaction \
      let x = (select balance, op1 from lab.accounts where customer = 1 and account_id = 1); \
      if x.balance >= 1 and x.op1 < {op} then \
      update lab.accounts set balance -= 1, op1 = {op} where customer = 1 and account_id = 1; \
      update lab.accounts set balance += 1 where customer = 1 and account_id = 2; \
      end if commit transaction;  -- assert ok or error
    select op1 from lab.accounts where customer = 1 and account_id = 1; -- assert ({{op}})
  }
}

w2: repeat 200 as op {
  retry 50 {
    begin transaction \
      let x = (select balance, op2 from lab.accounts where customer = 1 and account_id = 1); \
      if x.balance >= 1 and x.op2 < {op} then \
      update lab.accounts set balance -= 1, op2 = {op} where customer = 1 and account_id = 1; \
      update lab.accounts set balance += 1 where customer = 1 and account_id = 2; \
      end if commit transaction;  -- assert ok or error
    select op2 from lab.accounts where customer = 1 and account_id = 1; -- assert ({{op}})
  }
}

r1: repeat 1000 {
  begin transaction \
    let x = (select balance from lab.accounts where customer = 1 and account_id = 1); \
    let y = (select balance from lab.accounts where customer = 1 and account_id = 2); \
    select x.balance, y.balance; commit transaction;  -- assert sum(0, 1) = 2000
}

---

# atomicity: the pair holds 2000 units at every instant.
check: select balance from lab.accounts where customer = 1; -- assert sum(0) = 2000

# no lost updates: 400 transfers of 1 unit out of the payer.
check: select balance from lab.accounts where customer = 1 and account_id = 1; -- assert ({600})
check: select balance from lab.accounts where customer = 1 and account_id = 2; -- assert ({1400})
