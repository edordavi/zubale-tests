--- Date with most orders
select created_date, count(id) orders
    from tests.orders
    group by created_date
    order by orders desc
    limit 1
    ;

--- Most demanded product
select p.name product_name, sum(o.quantity) demand
    from tests.orders o
        join tests.products p
            on o.product_id = p.id
    group by p.name
    order by demand desc
    limit 1;

--- Top 3 most demanded categories
select p.category, sum(o.quantity) demand
    from tests.orders o
        join tests.products p
            on o.product_id = p.id
    group by p.category
    order by demand desc
    limit 3;