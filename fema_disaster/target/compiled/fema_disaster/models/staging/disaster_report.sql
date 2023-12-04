

with disaster_record as (
    select * from  fema_disaster.disasters
)

select count(disaster_number) as no_of_disaster, disaster_number, disaster_year
from disaster_record
group by disaster_year, disaster_number
order by disaster_year asc