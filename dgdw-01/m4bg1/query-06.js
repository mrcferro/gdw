var c = db.city.findOne({ city_geo: { $geoIntersects: { $geometry: { type: "Point", coordinates:[ -87.42, 41.24 ]}}}});

var result = db.c_address.aggregate([
   {
      $match: {
          c_address_geo: {
             $geoWithin: {
                $geometry: c.city_geo
             }
          }
      }
   },   
   { $project : { c_address_pk: 1} },   
   {	
       $lookup:
       {
           from: "customer",
           localField: "c_address_pk",
           foreignField: "c_address_fk",
           as: "customer_join"
       }
   },
   {
        $unwind: 
        { 
            path: "$customer_join", 
            preserveNullAndEmptyArrays: false
        }
    },   
   { $project : { "customer_join.c_custkey": 1, "customer_join.c_region": 1, "customer_join.c_nation": 1,"customer_join.c_city": 1 } }, 
   {	
       $lookup:
       {
           from: "lineorder",
           localField: "customer_join.c_custkey",
           foreignField: "lo_custkey",
           as: "lineorder_join"
       }
   }, 
   {
        $unwind: 
        { 
            path: "$lineorder_join", 
            preserveNullAndEmptyArrays: false
        }
   },	 
   { $project : { "lineorder_join.lo_partkey": 1, "lineorder_join.lo_quantity": 1, "customer_join.c_region": 1, "customer_join.c_nation": 1, "customer_join.c_city": 1  } },		
   {	
       $lookup:
       {
           from: "part",
           localField: "lineorder_join.lo_partkey",
           foreignField: "p_partkey",
           as: "part_join"
       }
   },   
   {
        $unwind: 
        { 
            path: "$part_join", 
            preserveNullAndEmptyArrays: false
        }  
   }, 
   { $project : {"lineorder_join.lo_quantity": 1, "customer_join.c_region": 1, "customer_join.c_nation": 1, "customer_join.c_city": 1, "part_join.p_category" : 1 } },	
   {
       $group:
       {
           _id : { category: "$part_join.p_category" ,  region: "$customer_join.c_region", nation: "$customer_join.c_nation", city: "$customer_join.c_city"},
           quantity: { $sum: "$lineorder_join.lo_quantity" }
       }
   }
]).toArray()

print("results = " + result.length)
printjson(result)
