select
  a.id as parcel_id,
  b.id as adj_parcel_id,
  st_dwithin(a.geom, b.geom,50) as adjacent
from
  (Select id, geom
   FROM rps.greenecounty) a
CROSS JOIN
  (Select id, geom
   FROM rps.greenecounty) b
where
  st_dwithin(a.geom, b.geom,50) = true
order by a.id
