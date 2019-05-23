SELECT p_category, c_region, c_nation, c_city, sum(lo_quantity) 
 FROM customer, city, lineorder, part
 WHERE 
   ST_Contains(city_geo, ST_GeomFromText('POINT(-87.42 41.24)') ) 
   and city_pk = c_city_fk
   and c_custkey = lo_custkey
   and lo_partkey = p_partkey
GROUP BY p_category, c_region, c_nation, c_city