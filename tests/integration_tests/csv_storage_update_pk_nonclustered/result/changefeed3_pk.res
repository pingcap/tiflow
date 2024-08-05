"I","update_pk","test",450250823741472787,1,"example1"
"I","update_pk","test",450250823741472787,2,"example2"
"I","update_pk","test",450250823741472790,10,"example10"
"I","update_pk","test",450250823741472790,20,"example20"
"I","update_pk","test",450250823741472791,100,"example100"
"I","update_pk","test",450250823741472792,1000,"example1000"

# output raw change event
"U","update_pk","test",450250823807270922,2,"example1"
"U","update_pk","test",450250823807270922,1,"example2"


# DIFF_RES:
# DELETE FROM `test`.`update_pk` WHERE `id` = 10 AND `pad` = 'example10' LIMIT 1;
# DELETE FROM `test`.`update_pk` WHERE `id` = 20 AND `pad` = 'example20' LIMIT 1;
# DELETE FROM `test`.`update_pk` WHERE `id` = 100 AND `pad` = 'example100' LIMIT 1;
# old row is not deleted since lack of old value
"U","update_pk","test",450250823807270925,30,"example10"
"U","update_pk","test",450250823807270925,40,"example20"
"U","update_pk","test",450250823807270927,200,"example100"

"U","update_pk","test",450250823807270928,1000,"example1001"