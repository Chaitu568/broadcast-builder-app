#! /bin/bash
#
cd source/
python3 generate_dependency_csvs.py
#aws s3 cp s3://mobivity-datascience/flight_datascience/bcast_list_builder/opt_in_source.csv /home/ec2-user/bbapp/source/opt_in_source.csv
#aws s3 cp s3://mobivity-datascience/flight_datascience/bcast_list_builder/state_names.csv /home/ec2-user/bbapp/source/state_names.csv
#aws s3 cp s3://mobivity-datascience/flight_datascience/bcast_list_builder/app_titles.csv /home/ec2-user/bbapp/source/app_titles.csv
#aws s3 cp s3://mobivity-datascience/flight_datascience/bcast_list_builder/bu_names.csv /home/ec2-user/bbapp/source/bu_names.csv
#aws s3 cp s3://mobivity-datascience/flight_datascience/bcast_list_builder/company_names.csv /home/ec2-user/bbapp/source/company_names.csv
