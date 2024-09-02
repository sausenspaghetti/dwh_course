-- создание staging слоя


----------------------------------------------------------------------------------
-- создание таблиц staging слоя
----------------------------------------------------------------------------------
drop table if exists staging.last_update;
create table staging.last_update (
	table_name varchar(100) not null,
	update_dt timestamp not null
);



drop table if exists staging.film;
create table staging.film (
    film_id int not null,
    title varchar(255) not null,
    description text null,
    release_year int2 null,
    language_id int2 not null,
    rental_duration int2 not null,
    rental_rate numeric(4,2) not null,
    length int2 null,
    replacement_cost numeric(5,2) not null,
    rating varchar(10) null,
    last_update timestamp not null,
    special_features _text null,
    fulltext tsvector not null
);



drop table if exists staging.inventory;
create table staging.inventory (
    inventory_id int4 not null,
    film_id int2 not null,
    store_id int2 not null,
    last_update TIMESTAMP not null,
    deleted TIMESTAMP null
);  



drop table if exists staging.rental;
create table staging.rental (
    rental_id int4 not null,
    rental_date timestamp not null,
    inventory_id int4 not null,
    customer_id int2 not null,
    return_date timestamp null,
    staff_id int2 not null
);



drop table if exists staging.payment;
create table staging.payment (
    payment_id int4 not null,
    customer_id int2 not null,
    staff_id int2 not null,
    rental_id int4 not null,
    amount numeric(5,2) not null,
    payment_date timestamp not null
);



drop table if exists staging.staff;
create table staging.staff (
    staff_id int4 NOT NULL,
    first_name varchar(45) NOT NULL,
    last_name varchar(45) NOT NULL,
    store_id int2 NOT NULL,
    deleted TIMESTAMP NULL,
	last_update timestamp NOT NULL
);



drop table if exists staging.address;
create table staging.address (
    address_id int4 NOT NULL,
    address varchar(50) NOT NULL,
    district varchar(20) NOT NULL,
    city_id int2 NOT NULL
);



drop table if exists staging.city;
CREATE TABLE staging.city (
    city_id int4 NOT NULL,
    city varchar(50) NOT NULL
);



drop table if exists staging.store;
CREATE TABLE staging.store (
    store_id integer NOT NULL,
    address_id int2 NOT NULL
);







----------------------------------------------------------------------------------
-- создание процедур загрузки данных в staging слой
----------------------------------------------------------------------------------
create or replace procedure staging.film_load()
as $$
    begin
        delete from staging.film;

        insert into
        staging.film
            (
                film_id,
                title,
                description,
                release_year,
                language_id,
                rental_duration,
                rental_rate,
                length,
                replacement_cost,
                rating,
                last_update,
                special_features,
                fulltext
            )
        select 
            film_id,
            title,
            description,
            release_year,
            language_id,
            rental_duration,
            rental_rate,
            length,
            replacement_cost,
            rating,
            last_update,
            special_features,
            fulltext
        from
            film_src.film;
    end;
$$ language plpgsql;



CREATE OR REPLACE PROCEDURE staging.inventory_load()
 LANGUAGE plpgsql
AS $procedure$
	declare
		last_update_dt timestamp;
    begin
		last_update_dt = coalesce(
			(
				select 
					max(lu.update_dt) 
				from 
					staging.last_update lu 
				where 
					lu.table_name = 'staging.inventory'
			),
			'1900-01-01'::date
		);
		
		delete from staging.inventory;		

        insert into staging.inventory
        (
            inventory_id, 
            film_id, 
            store_id,
            last_update,
			deleted
        )
        select 
            inventory_id, 
            film_id, 
            store_id,
            last_update,
			deleted
        from
            film_src.inventory i
		where	
			-- добавляем только записи о недавно удаленных
			-- 
			i.deleted > last_update_dt
			or i.last_update > last_update_dt
		;

		
		insert into staging.last_update 
		(
			table_name, 
			update_dt
		)
		values 
		(
			'staging.inventory', 
			now()
		);
		

    end;
$procedure$
;


create or replace procedure staging.rental_load()
as $$
    begin
        delete from staging.rental;

        insert into staging.rental
        (
            rental_id, 
            rental_date, 
            inventory_id, 
            customer_id, 
            return_date, 
            staff_id
        )
        select 
            rental_id, 
            rental_date, 
            inventory_id, 
            customer_id, 
            return_date, 
            staff_id
        from
            film_src.rental;
    end;
$$ language plpgsql;



create or replace procedure staging.payment_load()
as $$
    begin
        delete from staging.payment;

        insert into staging.payment
        (
            payment_id, 
            customer_id, 
            staff_id, 
            rental_id, 
            amount, 
            payment_date
        )
        select
            payment_id, 
            customer_id, 
            staff_id, 
            rental_id, 
            amount, 
            payment_date
        from
            film_src.payment;
    end;
$$ language plpgsql;




CREATE OR REPLACE PROCEDURE staging.staff_load()
 LANGUAGE plpgsql
AS $procedure$
	declare
		last_update_dt timestamp;
    begin
		-- последнее обновление
		last_update_dt = coalesce(
			(
				select 
					max(lu.update_dt) 
				from 
					staging.last_update lu 
				where 
					lu.table_name = 'staging.staff'
			),
			'1900-01-01'::date
		);
		
		-- очищаем таблицу staff
		delete from staging.staff;	

		-- наполняем ее новыми значениями
		insert into staging.staff
        (
            staff_id,
            first_name,
            last_name,
            store_id,
			last_update,
			deleted
        )
        select
            staff_id,
            first_name,
            last_name,
            store_id,
			last_update,
			deleted 
        from
            film_src.staff s
		where
			s.deleted > last_update_dt
			or s.last_update > last_update_dt
		;
		

		
		-- делаем запись о последнем обновлении таблицы
		insert into staging.last_update 
		(
			table_name, 
			update_dt
		)
		values 
		(
			'staging.staff', 
			now()
		);
		
    end;
$procedure$
;



create or replace procedure staging.address_load()
as $$
    begin 
        delete from staging.address;

        insert into staging.address
        (
            address_id,
            address,
            district,
            city_id
        )
        select
            address_id,
            address,
            district,
            city_id
        from 
            film_src.address;
    end;
$$ language plpgsql;



create or replace procedure staging.city_load()
as $$
    begin 
        delete from staging.city;

        insert into staging.city
        (
            city_id,
            city
        )
        select
            city_id,
            city
        from
            film_src.city;
    end;
$$ language plpgsql;



create or replace procedure staging.store_load()
as $$
    begin 
        delete from staging.store;
        insert into staging.store
        (
            store_id,
            address_id
        )
        select
            store_id,
            address_id
        from
            film_src.store;
    end;
$$ language plpgsql;







----------------------------------------------------------------------------------
-- загрузка всех данных из источника в staging
----------------------------------------------------------------------------------
create or replace procedure staging.update_staging()
as $$
    begin
        call staging.address_load();
        call staging.city_load();
        call staging.film_load();
        call staging.inventory_load();
        call staging.payment_load();
        call staging.rental_load();
        call staging.staff_load();
        call staging.store_load();
    end;
$$ language plpgsql;

