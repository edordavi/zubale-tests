create schema if not exists tests;

create table if not exists tests.orders
(
id integer,
product_id integer,
quantity integer,
created_date varchar(20)
);

create table if not exists tests.products
(
id integer,
name varchar(100),
category varchar(100),
price numeric(16,6)
);