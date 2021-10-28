use checkpoint_transaction;

start transaction;
    call dowhile1(30);
commit;