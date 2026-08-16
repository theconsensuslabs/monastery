# Transfers as bare writes in a conditional BATCH.
#
# customer is the partition key, so (1,1) and (2,1) are in
# different partitions.
# Paxos runs per partition and a conditional batch cannot span two,
# so every write here is rejected and the balances never move.
#
# The rows always exist, so `IF EXISTS` never rejects a write. It is
# there to make the batch conditional, which routes it through Paxos.
# The reader reads at SERIAL to match.

create keyspace if not exists lab with replication = {'class':'NetworkTopologyStrategy','datacenter1':3};
drop table if exists lab.accounts;
create table lab.accounts (customer int, account_id int, balance int, primary key (customer, account_id));
insert into lab.accounts (customer, account_id, balance) values (1, 1, 1000);
insert into lab.accounts (customer, account_id, balance) values (2, 1, 1000);

--- concurrent

w1: repeat 400 as x {
  begin batch \
    update lab.accounts set balance = {x} where customer = 1 and account_id = 1 if exists; \
    update lab.accounts set balance = 2000 - {x} where customer = 2 and account_id = 1; \
    apply batch;  -- assert error
}

w2: repeat 400 as x {
  begin batch \
    update lab.accounts set balance = 2000 - {x} where customer = 1 and account_id = 1 if exists; \
    update lab.accounts set balance = {x} where customer = 2 and account_id = 1; \
    apply batch;  -- assert error
}

r1: consistency serial;
r1: repeat 1500 {
  select balance from lab.accounts where customer in (1, 2); -- assert sum(0) = 2000
}

---

# atomicity and isolation: the pair totals 2000 at every instant.
check: select balance from lab.accounts where customer in (1, 2); -- assert sum(0) = 2000

# nothing applied, so the balances are untouched.
check: select balance from lab.accounts where customer = 1 and account_id = 1; -- assert ({1000})
check: select balance from lab.accounts where customer = 2 and account_id = 1; -- assert ({1000})
