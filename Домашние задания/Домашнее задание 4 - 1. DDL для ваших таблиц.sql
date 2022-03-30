create table kela_4_couriers (
	courier_id number (5, 0) not null,
	courier_name varchar2(20) not null,
	lastname varchar2(20) not null,
	constraint de3at_kela_4_couriers_PK primary key (courier_id));

create table kela_4_products (
	product_id number(10, 0) not null,
	product_name varchar2(50) not null,
	constraint de3at_kela_4_products_PK primary key (product_id));

create table kela_4_deliveries_products (
	delivery_id number(10, 0) not null,
	product_id number(10, 0) not null);

create table kela_4_deliveries (
	delivery_id number(10, 0) not null,
	courier_id number(5, 0) not null,
	buyer_id number(10, 0) not null,
	estimation number(1, 0) default 5 not null,
	constraint de3at_kela_4_deliveries_PK primary key (delivery_id));

create table kela_4_buyers (
	buyer_id number(10, 0) not null,
	buyer_name varchar2(20) not null,
	lastname varchar2(20) not null,
	constraint de3at_kela_4_buyers_PK primary key (buyer_id));

create table kela_4_deliveries_couriers (
	delivery_id number(10, 0) not null,
	courier_id number(10, 0) not null);

alter table kela_4_deliveries_products 
	add constraint de3at_k4_del_products_fk0 
	foreign key (delivery_id) 
	references kela_4_deliveries(delivery_id);
alter table kela_4_deliveries_products 
	add constraint de3at_k4_del_products_fk1 
	foreign key (product_id) 
	references kela_4_products(product_id);
alter table kela_4_deliveries 
	add constraint de3at_k4_del_fk0 
	foreign key (buyer_id) 
	references kela_4_buyers(buyer_id);
alter table kela_4_deliveries_couriers 
	add constraint de3at_k4_del_couriers_fk0 
	foreign key (delivery_id) 
	references kela_4_deliveries(delivery_id);
alter table kela_4_deliveries_couriers 
	add constraint de3at_k4_del_couriers_fk1 
	foreign key (courier_id) 
	references kela_4_couriers(courier_id);
