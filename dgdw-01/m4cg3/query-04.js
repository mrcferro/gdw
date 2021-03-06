var result = db.customer.aggregate([
   {
      $match: {
           c_address_geo: {
             $geoWithin: {
                   $centerSphere: [ [ -87.42, 41.24 ], 10/3963.2 ] 
             }
          }
      }
   },
   { $project : { "c_custkey": 1, "c_address": 1 } }, 
   {	
       $lookup:
       {
           from: "lineorder",
           localField: "c_custkey",
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
   { $project : { "c_address": 1, "lineorder_join.lo_partkey": 1, "lineorder_join.lo_quantity": 1  } },	
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
   { $project : { "part_join.p_category": 1, "lineorder_join.lo_quantity": 1, "c_address": 1 } }, 
   {
       $group:
       {
            _id : { category: "$part_join.p_category" ,  c_address: "$c_address"},
           quantity: { $sum: "$lineorder_join.lo_quantity" }
       }
   },

]).toArray()

print("results = " + result.length)
printjson(result)
