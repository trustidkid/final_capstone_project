

  create or replace view `adeairflow`.`fema_disaster`.`my_second_dbt_model`
  OPTIONS()
  as -- Use the `ref` function to select from other models

select *
from `adeairflow`.`fema_disaster`.`my_first_dbt_model`
where id = 1;

