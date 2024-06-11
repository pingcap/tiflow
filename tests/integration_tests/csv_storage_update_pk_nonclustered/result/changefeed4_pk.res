"I","update_pk","test",450250823741472787,false,1,"example1"
"I","update_pk","test",450250823741472787,false,2,"example2"
"I","update_pk","test",450250823741472790,false,10,"example10"
"I","update_pk","test",450250823741472790,false,20,"example20"
"I","update_pk","test",450250823741472791,false,100,"example100"
"I","update_pk","test",450250823741472792,false,1000,"example1000"

# split and sort in table sink
"D","update_pk","test",450250823807270922,false,1,"example1"
"D","update_pk","test",450250823807270922,false,2,"example2"
"I","update_pk","test",450250823807270922,false,2,"example1"
"I","update_pk","test",450250823807270922,false,1,"example2"

# split and sort in table sink
"D","update_pk","test",450250823807270925,false,10,"example10"
"D","update_pk","test",450250823807270925,false,20,"example20"
"I","update_pk","test",450250823807270925,false,30,"example10"
"I","update_pk","test",450250823807270925,false,40,"example20"

# split and sort in table sink
"D","update_pk","test",450250823807270927,false,100,"example100"
"I","update_pk","test",450250823807270927,false,200,"example100"

# normal update event, split in csv encoder
"D","update_pk","test",450250823807270928,true,1000,"example1000"
"I","update_pk","test",450250823807270928,true,1000,"example1001"