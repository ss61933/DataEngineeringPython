/*
truncate table orders;
truncate table order_items;
truncate table categories;
truncate table departments;
truncate table products;
truncate table categories;


select  o.order_id,o.order_date,o.order_customer_id,o.order_status,
count(ot.order_item_id),sum(ot.order_item_quantity),sum(ot.order_item_subtotal)
from orders o , order_Items ot
where o.order_id=ot.order_item_order_id
group by o.order_id,o.order_date,o.order_customer_id,o.order_status;

*/



with ordersi as (select   o.order_id,o.order_status,
				 o.order_date,ot.order_item_product_id,
				 p.product_category_id,p.product_name,p.product_price,
				count(ot.order_item_id) as cnt_order_item_id,
				 sum(ot.order_item_quantity) as sum_order_item_quantity,
				 sum(ot.order_item_subtotal) as sum_order_item_subtotal
from orders o , order_items ot , products p 
where o.order_id=ot.order_item_order_id
and  (ot.order_item_product_id=p.product_id)
				-- and p.product_name like 'Nike Women%'
group by o.order_id,o.order_status,o.order_date,
				 ot.order_item_product_id,p.product_category_id,p.product_name,p.product_price
)
select  
d.department_id,d.department_name,
c.category_id,c.category_name,
o.order_id,o.order_date,o.order_status
,o.product_name,o.product_price,
cnt_order_item_id,sum_order_item_quantity,sum_order_item_subtotal
from departments d 
inner join categories c
on (d.department_id=c.category_department_id )
inner join ordersi o
on (o.product_category_id=c.category_id)
where   d.department_name='Footwear' and c.category_name= 'Cardio Equipment'
--where o.order_id=5870
;
