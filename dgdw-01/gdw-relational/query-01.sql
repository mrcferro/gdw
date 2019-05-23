SELECT p_category, c_region, sum(lo_quantity) 
 FROM customer, region, lineorder, part
 WHERE 
   ST_Contains(region_geo, ST_GeomFromText('POINT(-87.42 41.24)') ) 
   and region_pk = c_region_fk
   and c_custkey = lo_custkey
   and lo_partkey = p_partkey
GROUP BY p_category, c_region
