var n = db.city.findOne({ 'nation.nation_geo': { $geoIntersects: { $geometry: { type: "Point", coordinates:[ -87.42, 41.24 ]}}}},{'nation.nation_geo':1});

var result = db.customer.aggregate([
   {
      $match: {
          c_address_geo: {
             $geoWithin: {
                $geometry: n.nation[0].nation_geo
             }
          }
      }
   },   
   { $project : { "c_custkey": 1, "c_region": 1, "c_nation": 1 } },   
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
   { $project : { "lineorder_join.lo_partkey": 1, "lineorder_join.lo_quantity": 1, "c_region": 1, "c_nation": 1   } },		
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
   { $project : {"lineorder_join.lo_quantity": 1, "c_region": 1, "c_nation": 1, "part_join.p_category" : 1 } },	
   {
       $group:
       {
            _id : { category: "$part_join.p_category" ,  region: "$c_region", nation: "$c_nation"},
           quantity: { $sum: "$lineorder_join.lo_quantity" }
       }
   },
]).toArray()

print("results = " + result.length)
printjson(result)