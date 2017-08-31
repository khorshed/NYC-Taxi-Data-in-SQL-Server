




		--select min([tpep_pickup_datetime]), max([tpep_pickup_datetime]) from [Yellow_tripdata_2010_12]
		
		insert into [Yellow_tripdata_2011_02] (vendor_id,
	       tpep_pickup_datetime,
	       tpep_dropoff_datetime,
	       passenger_count,
	       trip_distance,
	       pickup_longitude,
	       pickup_latitude,
	       rate_code_id,
	       store_and_fwd_flag,
	       dropoff_longitude,
	       dropoff_latitude,
	       payment_type,
	       fare_amount,
	       extra,
	       mta_tax,
	       tip_amount,
	       tolls_amount,
	       total_amount )
		select vendor_id,
	       tpep_pickup_datetime,
	       tpep_dropoff_datetime,
	       passenger_count,
	       trip_distance,
	       pickup_longitude,
	       pickup_latitude,
	       rate_code_id,
	       store_and_fwd_flag,
	       dropoff_longitude,
	       dropoff_latitude,
	       payment_type,
	       fare_amount,
	       extra,
	       mta_tax,
	       tip_amount,
	       tolls_amount,
	       total_amount  from [Yellow_tripdata_2010_12] where [tpep_pickup_datetime]> '2010-12-31 23:59:59.000';
		   
		   GO
		   
		   DELETE from [Yellow_tripdata_2010_12] where [tpep_pickup_datetime]> '2010-12-31 23:59:59.000';

		   GO
		   
		ALTER TABLE [Yellow_tripdata_2010_12] WITH CHECK ADD CONSTRAINT check_Yellow_tripdata_2010_12 
	CHECK(
	    [tpep_pickup_datetime] >= '2010-12-01'
	    AND [tpep_pickup_datetime] <= '2010-12-31 23:59:59.000');