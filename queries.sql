create database if not exists space_objects;

create table launches (
    launch_id INTEGER PRIMARY KEY,
    position TEXT,
    status TEXT, 
    launch_place TEXT,
    launch_data DATA
);

create table objects(
    object_id INTEGER PRIMARY KEY,
    launch_id INTEGER,
    name TEXT,
    model TEXT,
    operator': 'TEXT,
    FOREIGN KEY (launch_id) REFERENCES launches(launch_id)
);

SELECT * FROM launches;

SELECT launch_id, position, status 
FROM launches 
where status = 'retired';

SELECT * 
from launches 
where launch_data between '1999-02-26' and '2022-09-07';


select distinct 'status' 
from launches;


Select 'status', count(*) as quntity
from launches
group by 'status' 
having count(*) > 5 and count(*) < 100;  


select l.launch_id, l.status, o.name, o.model
from launches l
join objects o on o.launch_id = l.launch_id;


SELECt distinct operator 
from objects;


select l.launch_id, l.status, o.operator
from launches l
join objects o on l.launch_id = o.launch_id
where o.operator = 'Arabsat';


select *
from launches
where status = 'retired' and position = '2 E';


select * , 
    rank(*) over(partition by status order by launch_id) as order_number
from launches;


SELECT *
FROM launches
where launch_place > 4000 and launch_place < 6000
order by launch_data;



SELECT l.launch_id, o.name, o.model,l.launch_data, count(*) as quantity
from objects o
join launches l on l.launch_id = o.launch_id
group by model
having count(*) > 5 and count(*) < 50;