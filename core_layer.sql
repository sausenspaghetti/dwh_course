-- создание таблиц core слоя


----------------------------------------------------------------------------------
-- создание таблиц core слоя
----------------------------------------------------------------------------------
drop table if exists core.dim_date cascade;
create table core.dim_date
(
  date_dim_pk              INT PRIMARY KEY ,
  date_actual              DATE NOT NULL,
  epoch                    BIGINT NOT NULL,
  day_suffix               VARCHAR(4) NOT NULL,
  day_name                 VARCHAR(9) NOT NULL,
  day_of_week              INT NOT NULL,
  day_of_month             INT NOT NULL,
  day_of_quarter           INT NOT NULL,
  day_of_year              INT NOT NULL,
  week_of_month            INT NOT NULL,
  week_of_year             INT NOT NULL,
  week_of_year_iso         CHAR(10) NOT NULL,
  month_actual             INT NOT NULL,
  month_name               VARCHAR(9) NOT NULL,
  month_name_abbreviated   CHAR(3) NOT NULL,
  quarter_actual           INT NOT NULL,
  quarter_name             VARCHAR(9) NOT NULL,
  year_actual              INT NOT NULL,
  first_day_of_week        DATE NOT NULL,
  last_day_of_week         DATE NOT NULL,
  first_day_of_month       DATE NOT NULL,
  last_day_of_month        DATE NOT NULL,
  first_day_of_quarter     DATE NOT NULL,
  last_day_of_quarter      DATE NOT NULL,
  first_day_of_year        DATE NOT NULL,
  last_day_of_year         DATE NOT NULL,
  mmyyyy                   CHAR(6) NOT NULL,
  mmddyyyy                 CHAR(10) NOT NULL,
  weekend_indr             BOOLEAN NOT NULL
);

CREATE INDEX dim_date_date_actual_idx
  ON core.dim_date(date_actual);



drop table if exists core.dim_inventory cascade;
create table core.dim_inventory (
    inventory_pk serial primary key,
    inventory_id integer not null,
    film_id integer not null,
    title varchar(255) not null,
    rental_duration int2 not null,
    rental_rate numeric(4,2) not null,
    length int2,
    rating varchar(10),
	effective_date_from timestamp default to_date('1900-01-01', 'yyyy-mm-dd') not null,
	effective_date_to timestamp default to_date('9999-01-01', 'yyyy-mm-dd') not null,
	is_active boolean default true not null
);



drop table if exists core.dim_staff cascade;
create table core.dim_staff (
    staff_pk serial primary key,
    staff_id integer unique not null,
    first_name varchar(45) not null,
    last_name varchar(45) not null,
    address varchar(50) not null,
    district varchar(20) not null,
    city_name varchar(50) not null,
	
	effective_date_from timestamp default to_date('1900-01-01', 'yyyy-mm-dd') not null,
	effective_date_to timestamp default to_date('9999-01-01', 'yyyy-mm-dd') not null,
	is_active boolean default true not null
);



drop table if exists core.fact_payment cascade;
create table core.fact_payment (
    payment_pk serial primary key,
    payment_id integer not null,
    amount numeric(7,2) not null,
    payment_date_fk integer not null references core.dim_date(date_dim_pk),
    inventory_fk integer not null references core.dim_inventory(inventory_pk),
    staff_fk integer not null references core.dim_staff(staff_pk)
);



drop table if exists core.fact_rental cascade;
create table core.fact_rental (
    rental_pk serial primary key,
    rental_id integer not null,
    inventory_fk integer not null references core.dim_inventory(inventory_pk),
    staff_fk integer not null references core.dim_staff(staff_pk),
    rental_date_fk integer not null references core.dim_date(date_dim_pk),
    return_date_fk integer references core.dim_date(date_dim_pk),
    cnt int2 not null,
    amount numeric(7,2)
);



----------------------------------------------------------------------------------
-- создание процедур загрузки данных в core слой из staging слоя
----------------------------------------------------------------------------------
create or replace procedure core.load_date(sdate date, nm int)
as $$
    begin
	
        INSERT INTO core.dim_date
        SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_dim_id,
            datum AS date_actual,
            EXTRACT(EPOCH FROM datum) AS epoch,
            TO_CHAR(datum, 'fmDDth') AS day_suffix,
            TO_CHAR(datum, 'TMDay') AS day_name,
            EXTRACT(ISODOW FROM datum) AS day_of_week,
            EXTRACT(DAY FROM datum) AS day_of_month,
            datum - DATE_TRUNC('quarter', datum)::DATE + 1 AS day_of_quarter,
            EXTRACT(DOY FROM datum) AS day_of_year,
            TO_CHAR(datum, 'W')::INT AS week_of_month,
            EXTRACT(WEEK FROM datum) AS week_of_year,
            EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW-') || EXTRACT(ISODOW FROM datum) AS week_of_year_iso,
            EXTRACT(MONTH FROM datum) AS month_actual,
            TO_CHAR(datum, 'TMMonth') AS month_name,
            TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
            EXTRACT(QUARTER FROM datum) AS quarter_actual,
            CASE
                WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
                WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
                WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
                WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
                END AS quarter_name,
            EXTRACT(YEAR FROM datum) AS year_actual,
            datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS first_day_of_week,
            datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS last_day_of_week,
            datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
            (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
            DATE_TRUNC('quarter', datum)::DATE AS first_day_of_quarter,
            (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
            TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
            TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
            TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
            TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
            CASE
                WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE
                ELSE FALSE
                END AS weekend_indr
--        FROM (SELECT '2007-01-01'::DATE + SEQUENCE.DAY AS datum
--            FROM GENERATE_SERIES(0, 6940) AS SEQUENCE (DAY)
--            ORDER BY SEQUENCE.DAY) DQ
        FROM (SELECT sdate + SEQUENCE.DAY AS datum
            FROM GENERATE_SERIES(0, nm) AS SEQUENCE (DAY)
            ORDER BY SEQUENCE.DAY) DQ
        ORDER BY 1;

    end;
$$ language plpgsql;




CREATE OR REPLACE PROCEDURE core.load_inventory()
 LANGUAGE plpgsql
AS $procedure$
	begin 
		-- Надо разобраться с 3 категориями:
		-- 1) new
		-- 2) updated
		-- 3) deleted
		

		-------------------------
		-- 1) NEW
		-------------------------

		-- 1. получаем список идентификаторов новых компакт дисков
		create temporary table new_inventory_id_list on commit drop as 
		select
			i.inventory_id 
		from
			staging.inventory i 
			left join core.dim_inventory di using(inventory_id)
		where 
			di.inventory_id is null;

		-- 2. добавляем новые компакт диски в измерение dim_inventory
		insert
			into
			core.dim_inventory
		(
			inventory_id,
			film_id,
			title,
			rental_duration,
			rental_rate,
			length,
			rating,
			effective_date_from,
			effective_date_to,
			is_active 
		)
		select
			i.inventory_id,
			i.film_id,
			f.title,
			f.rental_duration,
			f.rental_rate,
			f.length,
			f.rating,
			
			'1900-01-01'::date as effective_date_from,
			coalesce(i.deleted, '9999-01-01'::date) as effective_date_to,
			case 
				when i.deleted is null then true 
				else false
			end as is_active
	
		from
			staging.inventory i
			join staging.film f using(film_id)
			join new_inventory_id_list idl using(inventory_id);





		-------------------------
		-- 2) UPDATED
		-------------------------
		-- 3. помечаем измененные компакт диски не активными
		update core.dim_inventory ii
		set
			is_active = false,
			effective_date_to = si.last_update 
		from 
			staging.inventory si
			left join new_inventory_id_list idl using(inventory_id)
		where 
			idl.inventory_id is null
			and si.deleted is null
			and ii.inventory_id = si.inventory_id 
			and ii.is_active is true;



		-- 4. по измененым компакт дискам добавляем актуальные строки
		insert into
			core.dim_inventory
		(
			inventory_id,
			film_id,
			title,
			rental_duration,
			rental_rate,
			length,
			rating,
			effective_date_from,
			effective_date_to,
			is_active 
		)
		select
			i.inventory_id,
			i.film_id,
			f.title,
			f.rental_duration,
			f.rental_rate,
			f.length,
			f.rating,
			i.last_update as effective_date_from,
			'9999-01-01'::date as effective_date_to,
			true as is_active
		from
			staging.inventory i
			join staging.film f using(film_id)
			left join new_inventory_id_list idl using(inventory_id)
		where 
			idl.inventory_id is null
			and i.deleted is null;

		
		
		-------------------------
		-- 3) DELETED
		-------------------------
		--  5. помечаем удаленные записи
		update core.dim_inventory i
		set 
			is_active = false,
			effective_date_to = si.deleted 
		from 
			staging.inventory si
			-- inner join core.dim_inventory i using(inventory_id)
			left join new_inventory_id_list idl using(inventory_id)
		where 
			si.deleted is not null
			and idl.inventory_id is null
			and i.inventory_id = si.inventory_id
			and i.is_active is true;


	end;
$procedure$;





CREATE OR REPLACE PROCEDURE core.load_staff()
 LANGUAGE plpgsql
AS $procedure$
	begin
		-- Надо разобраться с 3 категориями записей:
		-- 1) new
		-- 2) updated
		-- 3) deleted
		

		-------------------------
		-- 1) NEW
		-------------------------
		-- 1. находим список новых идентификаторов staff
		
--		drop table new_staff_id_list
--		create table new_staff_id_list as 
		
		create temporary table new_staff_id_list on commit drop as 
		select 
			distinct ss.staff_id as staff_id
		from
			staging.staff ss
			left join core.dim_staff cs using(staff_id)
		where
			cs.staff_id is null
		;
		
	
		-- 2. вставляем их в core.dim_staff
		INSERT INTO 
			core.dim_staff
		(
			staff_id, 
			first_name, 
			last_name, 
			address, 
			district, 
			city_name, 
			effective_date_from, 
			effective_date_to, 
			is_active
		)
		select
			ss.staff_id,
			ss.first_name,
			ss.last_name,
			a.address,
			a.district,
			c.city as city_name,
			'1900-01-01'::DATE as effective_date_from,
			coalesce(ss.deleted, '9999-01-01'::DATE) as effective_date_to,
			case
				when ss.deleted is null then true
				else false
			end as is_active
		
		from
			new_staff_id_list nsi
			
			inner join staging.staff ss using (staff_id)
			inner join staging.store st using(store_id)	
			inner join staging.address a using (address_id)
			inner join staging.city c using (city_id)
		;
		


	
		-------------------------
		-- 2) UPDATE
		-------------------------
		-- 3. Помечаем как неактивных всех обновленных сотрудников
		update
			core.dim_staff ds
		set
			is_active = false,
			effective_date_to = ss.last_update
		from 
			staging.staff ss
			left join new_staff_id_list nsi on nsi.staff_id = ss.staff_id
		 
		where
			nsi.staff_id is null 		-- не новые
			and ss.deleted is null 		-- не удаленные
			and ds.staff_id = ss.staff_id
		;	


		-- 4. Вставляем новые записи об обновленных сотрудниках
		INSERT INTO 
			core.dim_staff
		(
			staff_id, 
			first_name, 
			last_name, 
			address, 
			district, 
			city_name, 
			effective_date_from, 
			effective_date_to, 
			is_active
		)
		select
			ss.staff_id,
			ss.first_name,
			ss.last_name,
			a.address,
			a.district,
			c.city as city_name,
			ss.last_update as effective_date_from,
			'9999-01-01'::DATE as effective_date_to,
			true as is_active
		
		from
			staging.staff ss
	
			inner join staging.store st using(store_id)	
			inner join staging.address a using (address_id)
			inner join staging.city c using (city_id)
			left join new_staff_id_list nsi using (staff_id)

		where
			nsi.staff_id is null -- не новый
			and ss.deleted is null -- не удаленный
		;



		-------------------------
		-- 3) DELETED
		-------------------------
		--  5. помечаем удаленные записи
		update 
			core.dim_staff ds
		set 
			is_active = false,
			effective_date_to = ss.deleted 
		from 
			staging.staff ss
			left join new_staff_id_list nsi using(staff_id)
		where 
			ss.deleted is not null				-- удаленные
			and nsi.staff_id is null			-- не новые
			and ds.staff_id = ss.staff_id
			and ds.is_active is true
		;

		

	end;
$procedure$
;


create or replace procedure core.load_payment()
as $$
	begin
		delete from core.fact_payment;

		INSERT INTO core.fact_payment
		(
			payment_id,
			amount,
			payment_date_fk, 
			inventory_fk,
			staff_fk
		)
		select
			p.payment_id,
			sum(p.amount) as amount,
			dd.date_dim_pk as payment_date_fk,
			di.inventory_pk as inventory_fk,
			ds.staff_pk as staff_fk
			
		from
			staging.payment p

			inner join staging.rental rn
			on rn.rental_id = p.rental_id
		
			inner join core.dim_inventory di
			on di.inventory_id = rn.inventory_id

			inner join core.dim_staff ds
			on ds.staff_id = p.staff_id

			inner join core.dim_date dd
			on p.payment_date::date = dd.date_actual
			
		group by
			p.payment_id,
			date_dim_pk,
			di.inventory_pk,
			ds.staff_pk;
			
		
	end;
$$ language plpgsql;



create or replace procedure core.load_rental()
as $$
	begin 
		delete from core.fact_rental;
		
		insert into core.fact_rental
		(
			rental_id,
			inventory_fk,
			staff_fk,
			rental_date_fk,
			return_date_fk,
			amount,
			cnt
		)
		select
			r.rental_id,
			i.inventory_pk as inventory_fk,
			s.staff_pk as staff_fk,
			dd1.date_dim_pk as rental_date_fk,	
			dd2.date_dim_pk as return_date_fk,			

			sum(p.amount) as amount,
			count(*) as cnt
		from
			staging.rental r
		
			inner join core.dim_date dd1 on r.rental_date::date = dd1.date_actual

			inner join core.dim_inventory i using (inventory_id)
			inner join core.dim_staff s on s.staff_id = r.staff_id 
			left join staging.payment p using (rental_id)
			left join core.dim_date dd2 on r.return_date::date = dd2.date_actual

		group by
			r.rental_id,
			i.inventory_pk,
			s.staff_pk,
			dd1.date_dim_pk,
			dd2.date_dim_pk;

	
	end;
$$ language plpgsql;



create or replace procedure core.fact_delete()
as $$
	begin
		delete from core.fact_payment;
		delete from core.fact_rental;
	end
$$ language plpgsql;




----------------------------------------------------------------------------------
-- загрузка всех данных: staging --> core
----------------------------------------------------------------------------------
create or replace procedure core.update_core()
as $$
    begin
        -- Наполняем слой core
        call core.fact_delete();
        call core.load_inventory();
        call core.load_staff();
        call core.load_payment();
        call core.load_rental();
    end;
$$ language plpgsql;




----------------------------------------------------------------------------------
-- загрузка всех данных: источник --> staging --> core
----------------------------------------------------------------------------------
create or replace procedure core.full_load()
as $$
    begin
        call staging.update_staging();
        call core.update_core();
    end;
$$ language plpgsql;






