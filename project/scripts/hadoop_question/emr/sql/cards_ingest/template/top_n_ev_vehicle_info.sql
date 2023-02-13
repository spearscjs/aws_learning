
-- 3. Write a sql to show top 3 by number of vehicle registered. [ sql should be in file. the number to pas is dynamic. it can be 3 is can be 5]

select make, count(1) num_registered_vehicles
from cards_ingest.ev_vehicle_info
GROUP BY make
ORDER BY num_registered_vehicles DESC
LIMIT ${n};