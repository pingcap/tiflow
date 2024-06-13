"I","update_pk","test",450250823741472787,false,1,"example1"
"I","update_pk","test",450250823741472787,false,2,"example2"
"I","update_pk","test",450250823741472790,false,10,"example10"
"I","update_pk","test",450250823741472790,false,20,"example20"
"I","update_pk","test",450250823741472791,false,100,"example100"
"I","update_pk","test",450250823741472792,false,1000,"example1000"

# split in csv encoder
# DIFF_RES: REPLACE INTO `test`.`update_pk`(`id`,`pad`) VALUES (2,'example1');
# lost id=2 since delete are not sorted before insert within single txn
"D","update_pk","test",450250823807270922,true,1,"example1"
"I","update_pk","test",450250823807270922,true,2,"example1"
"D","update_pk","test",450250823807270922,true,2,"example2"
"I","update_pk","test",450250823807270922,true,1,"example2"

# split in csv encoder
"D","update_pk","test",450250823807270925,true,10,"example10"
"I","update_pk","test",450250823807270925,true,30,"example10"
"D","update_pk","test",450250823807270925,true,20,"example20"
"I","update_pk","test",450250823807270925,true,40,"example20"

# split in csv encoder
"D","update_pk","test",450250823807270927,true,100,"example100"
"I","update_pk","test",450250823807270927,true,200,"example100"

# normal update event, also split in csv encoder
"D","update_pk","test",450250823807270928,true,1000,"example1000"
"I","update_pk","test",450250823807270928,true,1000,"example1001"