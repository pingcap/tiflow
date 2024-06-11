"I","update_pk","test",450253245302439944,1,"example1"
"I","update_pk","test",450253245302439944,2,"example2"
"I","update_pk","test",450253245302439946,10,"example10"
"I","update_pk","test",450253245302439946,20,"example20"
"I","update_pk","test",450253245302439947,100,"example100"
"I","update_pk","test",450253245302439948,1000,"example1000"

# output raw change event
# translate to normal update in upstream
"U","update_pk","test",450253245485940746,1,"example2"
"U","update_pk","test",450253245485940746,2,"example1"

# split and sort in upstream
"D","update_pk","test",450253245485940749,10,"example10"
"D","update_pk","test",450253245485940749,20,"example20"
"I","update_pk","test",450253245485940749,30,"example10"
"I","update_pk","test",450253245485940749,40,"example20"

# split and sort in upstream
"D","update_pk","test",450253245485940752,100,"example100"
"I","update_pk","test",450253245485940752,200,"example100"

# normal update event
"U","update_pk","test",450253245485940753,1000,"example1001"