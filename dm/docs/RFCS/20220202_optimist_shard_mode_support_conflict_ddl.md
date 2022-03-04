# Proposal: DM Optimist Shard Mode Support Conflict DDL

- Author(s):    [gmhdbjd](https://github.com/gmhdbjd)
- Last updated: 2022-03-03

## Background

Current optimist shard mode does not support three types of DDLs.

- DDLs that make a table "big and small", such as `alter table tb rename column a to b`, which in a sense is equivalent to deleting `a` and adding `b` to table `tb`, making the table "big and small".
- DDLs that make the table incomparable, such as `alter table tb change a from int to varchar`, make it impossible to compare the size of `varchar` and `int`. For this kind of DDL, we can resolve it according to TiDB's fieldTypeMergeRules case by case, but we still need to consider a generic solution in order to be as compatible as possible with all DDLs. In a sense, this DDL also makes the table "big and small" (remove `a int`, add `a varchar`)
- Adding NotNULL NoDefault columns, such as `alter table tb add column a not null`, which also makes the table incomparable (INT NOT NULL No Default vs Nil). In a sense, this DDL also makes the table "big and small" (remove nil, add `a int not null`)

## Design

### Compare DDLs based on whether they are identical or not

A simple solution is to simply check whether the DDLs are the same, just as what we do for pessimistic DDL coordination. Coordination is completed when all sharding tables encounter the same conflicting DDL. This approach has the following problems.

- If the sharding DDLs are not identical (e.g. different comment information), then the coordination will fail
- As with pessimistic reconciliation, there may be problems with failover cases
  - DDL1, DDL2 are executed on split table 1 and 2
  - master coordination of DDL1 is complete
  - worker1 executes DDL1 successfully, then try to coordinate DDL2
  - worker2 crashes while executing DDL1 (DDL and checkpoint are not atomic), restart and recoordinate DDL1
  - master receives DDL2 from worker1, DDL1 from worker2, an error will be reported
- Since our optimistic coordination allows inconsistent initial table structures, we also encounter the following problem.
  - initial structure tb1(a int, b int), tb2(a int, b varchar(4))
  - tb1 execute `modify b varchar(4)`
  - at this point, all the sub-tables are consistent, but the master only receives the DDL for tb1, so coordination fails

### Comparison based on the schema before and after DDL execution

Instead of considering specific DDLs, this approach determines whether all the sharding tables have been reconciled based on the comparison of the schema before and after the DDL execution.

#### Consistency condition for sharding tables

Just to be clear, for a conflicting DDL that makes the table structure "big and small", suppose it makes table "-a, +b". When all the sharding tables have no `a` and have `b` at some point, then the DML synchronization of that sharding table can continue. e.g.

- If `rename column a to b`, when all sharding tables `rename column a to b`, then there is no conflict
- If `change a int to varchar`, when all sharding tables `change a int to varchar`, then there is no conflict
- If `add a int not null`, when all sharding tables `add a int not null`, then there is no conflict
- If table 1 `rename column a to b`, table 2 `drop column a, add column b`, then the downstream table structure has no `a`, has `b`, all the sharding tables can continue to synchronize DML, ***but the upstream and downstream data may be inconsistent***
- If table 1 `change a int to varchar`, table 2 `drop column a, add column a varchar`, then all tables can continue to synchronize DML, ***but the upstream and downstream data may be inconsistent***
- If table 1 `add a int not null`, `table 2 add a int not null default 0`, then all sharding tables can continue to synchronize DML.

***For the above possible data inconsistency, similar to the user did a truncate operation on a column of a sharding table, unless the user redo the task, the upstream and downstream data must be inconsistent, and there is no way to avoid this situation.***

#### Definitions

To determine whether the sharding tables consistent, the following operations and variables are defined.

#### Operations

These two operations are currently implemented by Optimist Shard Mode.

- Join(tb1,tb2,... ,tbN): the join operation, such as 
  - Join(tb1(a,b), tb2(a,c)) = tb(a,b,c)
  - Join(tb1(a int), tb2(a bigint)) = tb(a bigint)
  - Join(tb1(a int), tb2(a varchar)): Error
- Compare(tb1,tb2): compare operations, such as 
  - Compare(tb1(a,b), tb2(a,b,c)) = -1
  - Compare(tb1(a,b), tb2(a,b)) = 0
  - Compare(tb1(a bigint), tb2(a int)) = 1
  - Compare(tb1(a int), tb2(a varchar)): Error
  - Compare(tb1(a,b), tb2(a,c)): Error
  - Compare(tb1(a int, b int not null), tb2(a int)): Error

#### Variables

- prev_table: the table structure before a conflicting DDL is executed for a sharding table, or after being executed if the DDL is not conflicting
- post_table: the table structure of a sharding table after a conflicting DDL is executed
- prev_tables: the set of prev_table of all sharding tables
- post_tables: the set of post_tables for all sharding tables that received conflicting DDLs (if a sharding table did not receive a conflicting DDL, then that sharding table will not be included in the post_tables)
- final_tables: post_tables + {all table in prev_tables but not in post_tables}, that is: prev_table if a sharding table does not receive conflicting DDLs, and post_table if it receives conflicting DDLs
- joined: Join(final_tables), i.e. the set of all final_tables, i.e. the downstream joined table structure

#### Algorithm

We implement the final coordination algorithm according to the above definition.

##### Initialization

```golang
// At the beginning, prev_tables is equal to the table structure of all sharding tables in the checkpoint
prev_tables = {prev_tb1, prev_tb2,... ,prev_tbN}
post_tables = {}
final_tables = {prev_tb1, prev_tb2,... ,prev_tbN} = prev_tables
```

##### Maintenance

When master receives a DDL for sharding table `X`, `compare(prev_tbx,post_tbx)` to find out if the DDL is a conflicting DDL, e.g. 

- alter table add column new_col1, Compare(prev_tbx,post_tbx) = 1
- alter table rename a to b, Compare(prev_tbx,post_tbx) = error
- alter table change a from int to varchar, Compare(prev_tbx,post_tbx) = error
- alter table add column a int not null, Compare(prev_tbx,post_tbx) = error

If the DDL is not a conflicting DDL, just update the `tbx` in `prev_tables` and coordinate it normally.  
If the DDL is a conflicting DDL, add `post_tbx` to `post_tables`, update `final_tables`, i.e.

```golang
// Receive conflict DDL for sharding table X, get prev_tbx, post_tbx
prev_tables = {prev_tb1,... ,prev_tbx,... ,prev_tbN}
post_tables = {post_tbx}
final_tables = {prev_tb1,... ,post_tbx,... ,prev_tbN}
```

##### Termination

master will then receive conflicting DDLs for the different sharidng tables, keeping `post_tables` and `final_tables` updated.

```golang
// M <= N
prev_tables = {prev_tb1, prev_tb2,...,prev_tbN}
post_tables = {post_tb1,post_tb3,...,post_tbM}
final_tables = {post_tb1, prev_tb2, post_tb3,...,post_tbM,...,prev_tbN}
joined=Join(final_tables)
```

Coordination of all sharding tables is completed when the following condition is finally reached.

- `joined ! = error`
- `Compare(joined,prev_tbx) == error` for any sharding table `X` in `post_tables`
- for any sharding table `X` in `post_tables`, for any sharding table `Y` in `final_tables`: `Compare(Join(prev_tbx,final_tabley),post_tbx) >= 0`

##### Proof

We just need to prove that the final state of Termination satisfies the goal we proposed earlier, i.e., for each conflicting DDL(-a,+b), Termination's final end condition satisfies: all sharding tables (final_tables) have no `a` but `b`

Assume that sharding table `K` executes a DDL1 such that tbk(-a,+b), i.e., *a ∈ prev_tbk*, *b ∉ prev_tbk*, *a ∉ post_tbk*, *b ∈ post_tbk*, and *post_tbk = prev_tbk - {a} + {b}* ①

1. joined ! = error  

    i.e. all the sharding tables (final_tables) have no conflicts, substituting *table=k* gives *(joined=Join(final_tables)=Join(final_tbk,...) =Join(post_tbk,...)*  
    ⟶ *Compare(joined,post_tbk)=Compare(Join(post_tbk,...) ,post_tbk)>=0*  
    ⟶ *Compare(joined,post_tbk)>=0*, i.e. joined is greater than or equal to *post_tbk*, and substituting ① gives  
    ⟶ *Compare(joined,prev_tbk-{a}+{b})>=0* ②  

2.  for any sharding table X in post_tables, *Compare(joined,prev_tbx) == error*  

    ⟶ Substituting table=k gives *Compare(joined,prev_tbk) == error* ③  
    ②③ shows that joined contains *prev_tbk-{a}+{b}*, but not *prev_tbk*  
    ⟶ joined does not contain {a}, i.e., all sharding tables (final_tables) do not have {a}  

3. for any sharding table X in post_tables, any sharding table Y in final_tables, *Compare(Join(prev_tbx,final_tabley),post_tbx)>=0*  

    Substituting *table=k*, we get  
    ⟶ *Compare(Join(prev_tbk,final_tabley),post_tbk)>=0*  
    ⟶ *Compare(Join(prev_tbk,final_tabley),prev_tbk-{a}+{b})>=0*  
    ⟶ *Join(prev_tbk,final_tabley)* contains *prev_tbk-{a}+{b}*, i.e., final_tabley contains {b}    
    ⟶ All sharding tables (final_tables) have {b}  

We proved above that all the sharding tables in the final_tables do not have `a` but `b`. That is, the synchronization can continue when the final_tables meet the above three conditions.
