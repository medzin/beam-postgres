create table if not exists source (
    id int primary key generated always as identity,
    data varchar(255) not null
);

create table if not exists sink (
    id int primary key generated always as identity,
    data varchar(255) not null
);

insert into source (data) values ('test1');
insert into source (data) values ('test2');
insert into source (data) values ('test3');
insert into source (data) values ('test4');
insert into source (data) values ('test5');
