---DE FINAL PROJECT

create table users (
	"id" int,
	"first_name" varchar(255),
	"last name" varchar(255),
	"street_address" varchar(255),
	"postal code" varchar(255),
	"state" varchar(255),
	"city" varchar(255),
	"country" varchar(255),
	);

create table orders (
	"order_id" int,
	"status" varchar(255),
	"shipped_at" date,
	"delivered_at" date,
	"shipping_time" int
	);

create table order_items (
	"id" int not null,
	"order_id" int,
	"user_id" int,
	"product_id" int
	);

create table products (
	"id" int not null,
	"category" varchar(255),
	"name" varchar(255),
	"distribution_center_id" varchar(255),
	"city_name" varchar(255)
	);


