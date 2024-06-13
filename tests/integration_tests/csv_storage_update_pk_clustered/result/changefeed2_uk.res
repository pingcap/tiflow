"I","update_uk","test",450253245446619144,false,1,1,"example1"
"I","update_uk","test",450253245446619144,false,2,2,"example2"
"I","update_uk","test",450253245446619146,false,10,10,"example10"
"I","update_uk","test",450253245446619146,false,20,20,"example20"
"I","update_uk","test",450253245446619147,false,100,100,"example100"
"I","update_uk","test",450253245446619148,false,1000,1000,"example1000"

# split in csv encoder, data is consistent since delete by pk
"D","update_uk","test",450253245499047940,true,1,1,"example1"
"I","update_uk","test",450253245499047940,true,1,2,"example1"
"D","update_uk","test",450253245499047940,true,2,2,"example2"
"I","update_uk","test",450253245499047940,true,2,1,"example2"

# split in csv encoder
"D","update_uk","test",450253245499047943,true,10,10,"example10"
"I","update_uk","test",450253245499047943,true,10,30,"example10"
"D","update_uk","test",450253245499047943,true,20,20,"example20"
"I","update_uk","test",450253245499047943,true,20,40,"example20"

# split in csv encoder
"D","update_uk","test",450253245499047946,true,100,100,"example100"
"I","update_uk","test",450253245499047946,true,100,200,"example100"

# normal update event, also split in csv encoder
"D","update_uk","test",450253245512155140,true,1000,1000,"example1000"
"I","update_uk","test",450253245512155140,true,1000,1000,"example1001"