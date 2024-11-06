"I","update_uk","test",450250823780794385,false,1,1,"example1"
"I","update_uk","test",450250823780794385,false,2,2,"example2"
"I","update_uk","test",450250823780794387,false,10,10,"example10"
"I","update_uk","test",450250823780794387,false,20,20,"example20"
"I","update_uk","test",450250823780794389,false,100,100,"example100"
"I","update_uk","test",450250823780794390,false,1000,1000,"example1000"

# split in csv encoder, data is consistent since delete by pk
"D","update_uk","test",450250823807270931,true,1,1,"example1"
"I","update_uk","test",450250823807270931,true,1,2,"example1"
"D","update_uk","test",450250823807270931,true,2,2,"example2"
"I","update_uk","test",450250823807270931,true,2,1,"example2"

# split in csv encoder
"D","update_uk","test",450250823820115970,true,10,10,"example10"
"I","update_uk","test",450250823820115970,true,10,30,"example10"
"D","update_uk","test",450250823820115970,true,20,20,"example20"
"I","update_uk","test",450250823820115970,true,20,40,"example20"

# split in csv encoder
"D","update_uk","test",450250823820115973,true,100,100,"example100"
"I","update_uk","test",450250823820115973,true,100,200,"example100"

# normal update event, also split in csv encoder
"D","update_uk","test",450250823820115977,true,1000,1000,"example1000"
"I","update_uk","test",450250823820115977,true,1000,1000,"example1001"