package entry

import (
	"log"
	"testing"

	"github.com/hamba/avro"
)

type Person struct {
	Name      string `json:"name"`
	Age       int    `json:"age"`
	Height    int    `json:"height"`
	Weight    int    `json:"weight"`
	Address   string `json:"address"`
	Address1  string `json:"address1"`
	Address2  string `json:"address2"`
	Address3  string `json:"address3"`
	Address4  string `json:"address4"`
	Address5  string `json:"address5"`
	Address6  string `json:"address6"`
	Address7  string `json:"address7"`
	Address8  string `json:"address8"`
	Address9  string `json:"address9"`
	Address10 string `json:"address10"`
}

func getSchema() *avro.Schema {
	var schema_string string = `{
		"type":"record",
		"name":"person",
		"fields":[
		 {
			"name":"Name",
			"type":"string"
		  },
		 {
			"name":"Age",
			"type":"int"
		 },
		 {
			"name":"Height",
			"type":"int"
		 },
		 {
			"name":"Weight",
			"type":"int"
		 },
		 {
			"name":"Address",
			"type":"string"
		  },
		  {
			 "name":"Address1",
			 "type":"string"
		   },
		   {
			  "name":"Address2",
			  "type":"string"
			},
			{
			   "name":"Address3",
			   "type":"string"
			 },
			 {
				"name":"Address4",
				"type":"string"
			  },
			  {
				 "name":"Address5",
				 "type":"string"
			   },
			   {
				  "name":"Address6",
				  "type":"string"
				},
				{
				   "name":"Address7",
				   "type":"string"
				 },
				 {
					"name":"Address8",
					"type":"string"
				  },
				  {
					 "name":"Address9",
					 "type":"string"
				   },
				   {
					  "name":"Address10",
					  "type":"string"
					}
		  
		]
	 }`

	schema, err := avro.Parse(schema_string)
	if err != nil {
		panic(err)
	}
	return &schema
}

func encodePerson(schema *avro.Schema) []byte {
	var john *Person = &Person{
		Name:      "john",
		Age:       36,
		Height:    169,
		Weight:    169,
		Address:   "1234 Main St",
		Address1:  "1234 Main St",
		Address2:  "1234 Main St",
		Address3:  "1234 Main St",
		Address4:  "1234 Main St",
		Address5:  "1234 Main St",
		Address6:  "1234 Main St",
		Address7:  "1234 Main St",
		Address8:  "1234 Main St",
		Address9:  "1234 Main St",
		Address10: "1234 Main St",
	}

	avro_bytes, err := avro.Marshal(*schema, john)
	if err != nil {
		panic(err)
	}
	return avro_bytes
}

func BenchmarkDecoding(b *testing.B) {
	schema := getSchema()
	value := encodePerson(schema)
	for i := 0; i < b.N; i++ {
		p := Person{}
		err := avro.Unmarshal(*schema, value, &p)
		if err != nil {
			log.Fatal(err)
		}
	}
}
