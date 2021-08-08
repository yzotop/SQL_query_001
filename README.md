with clsrt_order_roll (date_order_roll, platform, program_id, quantity_clsrt_order_roll) as
         (
             select date                     date_order_roll,
                    platform,
                    case
                        when CAST(regexp_substr(event_data, E'(?:"program_id":")([0-9]+)', 1, 1, '', 1) AS integer) IS NOT NULL
                            then CAST(regexp_substr(event_data, E'(?:"program_id":")([0-9]+)', 1, 1, '', 1) AS integer)
                        when CAST(regexp_substr(event_data, E'(?:"program_id":)([0-9]+)', 1, 1, '', 1) AS integer) IS NOT NULL
                            then CAST(regexp_substr(event_data, E'(?:"program_id":)([0-9]+)', 1, 1, '', 1) AS integer)
                        end as               program_id,
                    count(distinct sid_long) quantity_clsrt_order_roll
             from netology.clickstream
             where 1 = 1
               and (date = '2021-03-01')
               and type = 'event'
               and event_value ilike '/virtual/paid%' -- только платные
               and event_type = 'virtual_page'
               and platform in ('mobile', 'pc')
               and regexp_like(page_url, 'order')
             group by 1, 2, 3
         )

     clsrt_paid (date, platform, program_id, quantity_paid) as
         (
             select date,
                    platform,
                    case
                        when CAST(regexp_substr(event_data, E'(?:"program_id":")([0-9]+)', 1, 1, '', 1) AS integer) IS NOT NULL
                            then CAST(regexp_substr(event_data, E'(?:"program_id":")([0-9]+)', 1, 1, '', 1) AS integer)
                        when CAST(regexp_substr(event_data, E'(?:"program_id":)([0-9]+)', 1, 1, '', 1) AS integer) IS NOT NULL
                            then CAST(regexp_substr(event_data, E'(?:"program_id":)([0-9]+)', 1, 1, '', 1) AS integer)
                        end as               program_id,
                    count(distinct sid_long) quantity_paid
             from netology.clickstream
             where 1 = 1
               and (date = '2021-03-01')
               and type = 'event'
               and event_value ilike '/virtual/paid%' -- только платные
               and event_type = 'virtual_page'
               and platform in ('mobile', 'pc')
             group by 1, 2, 3
         ),


     cosh (date_lead, program_id, leads) as
         (
             select cart_items_created_at::DATE date_lead,
                    resource_id                 program_id,
                    count(resource_id)          leads
             from netology_mart.carts_and_orders_short
             where 1 = 1
               and (cart_items_created_at::DATE = '2021-03-01')
               and state in ('success', 'false')
               and price > 20000
             group by 1, 2
         )
