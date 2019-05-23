SELECT p_category, c_address, sum(lo_quantity) 
 FROM customer, c_address, lineorder, part
 WHERE 
   ST_Distance_Sphere(c_address_geo, ST_MakePoint(-87.42, 41.24)) <= 10 * 1609.34
   and c_address_pk = c_address_fk
   and c_custkey = lo_custkey
   and lo_partkey = p_partkey
GROUP BY p_category, c_address
   

