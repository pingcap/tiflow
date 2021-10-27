use checkpoint_transaction;

start transaction;
    call dowhile2(30);
commit;