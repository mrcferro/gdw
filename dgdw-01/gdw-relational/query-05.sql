SELECT p_category, c_region, c_nation, sum(lo_quantity) 
 FROM customer, nation, lineorder, part
 WHERE 
   ST_Contains(nation_geo, ST_GeomFromText('POINT(-87.42 41.24)') ) 
   and nation_pk = c_nation_fk
   and c_custkey = lo_custkey
   and lo_partkey = p_partkey
GROUP BY 
   p_category, c_region, c_nation