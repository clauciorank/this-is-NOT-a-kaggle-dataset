create table datasets (
dataset_url varchar,
dataset_slug varchar,
owner_name varchar,
owner_user_id int,
date_created int,
date_updated int,
dataset_id varchar,
dataset_title varchar
)

create table categories (
dataset_id varchar,
category_id varchar,
category_name varchar,
dataset_count int,
competition_count int,
notebook_count int
)

create table dataset_activity(
	dataset_id varchar,
	views int,
	downloads int,
	download_per_view float,
	unique_contributors int,
	notebooks int
)

create table downloads_timeseries(
dataset_id varchar,
date timestamp,
count int
)

create table views_timeseries(
dataset_id varchar,
date timestamp,
count int
)

create table databundle_id(
dataset_id varchar,
databundle_id varchar
)

create table usability_rating(
	dataset_id varchar,
	score float,
	column_description_score float,
	cover_image_score float,
	file_description_score float,
	file_format_score float,
	license_score float,
	overview_score float,
	public_kernel_score float,
	subtitle_score float,
	provenance_score float,
	tag_score float,
	update_frequency_score float
)


select * from usability_rating 

select * from dataset_activity 


select * from datasets d 


select * from downloads_timeseries

select * from categories c 
