"I","update_pk","test",450253245302439944,false,1,"example1"
"I","update_pk","test",450253245302439944,false,2,"example2"
"I","update_pk","test",450253245302439946,false,10,"example10"
"I","update_pk","test",450253245302439946,false,20,"example20"
"I","update_pk","test",450253245302439947,false,100,"example100"
"I","update_pk","test",450253245302439948,false,1000,"example1000"

# translate to normal update in upstream, split in csv encoder
"D","update_pk","test",450253245485940746,true,1,"example1"
"I","update_pk","test",450253245485940746,true,1,"example2"
"D","update_pk","test",450253245485940746,true,2,"example2"
"I","update_pk","test",450253245485940746,true,2,"example1"

# split and sort in upstream
"D","update_pk","test",450253245485940749,false,10,"example10"
"D","update_pk","test",450253245485940749,false,20,"example20"
"I","update_pk","test",450253245485940749,false,30,"example10"
"I","update_pk","test",450253245485940749,false,40,"example20"

# split and sort in upstream
"D","update_pk","test",450253245485940752,false,100,"example100"
"I","update_pk","test",450253245485940752,false,200,"example100"

# normal update event, split in csv encoder
"D","update_pk","test",450253245485940753,true,1000,"example1000"
"I","update_pk","test",450253245485940753,true,1000,"example1001"
