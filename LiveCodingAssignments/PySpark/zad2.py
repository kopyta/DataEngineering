import logging

from pyspark.sql import SparkSession
import pyspark.sql.functions as f


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)-19s] [%(levelname)-8s] %(name)s: %(message)s ',
        datefmt='%Y-%m-%d %H:%M:%S',
        filename='./zadanie2.log',
    )
    _logger = logging.getLogger('__main__')
    spark = (
        SparkSession.builder
        .appName("HDFS")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
      
    _logger.info('===== 2.2a =====')
    _logger.info('Reading files...')
    df_wifi = spark.read\
        .options(header='true', inferschema='true')\
        .csv('hdfs://localhost:8020/user/vagrant/kolos/csv_data/WIFI.csv')
    _logger.info('done :)')
    
    _logger.info('===== 2.2b =====')
    _logger.info(f'schemat:')
    _logger.info(f'{df_wifi.schema}')  
    _logger.info(f'Ilosc rekordów: {df_wifi.count()}')
    _logger.info(f'Suma wartosci w kolumnie COUNTDIST: {df_wifi.agg(Map("COUNTDIST" -> "sum")).collect()(0)(0)}')
    _logger.info('===== 2.2c =====')
    _logger.info(f'dwuwymiarowa siatka:')
    df_rounded2 = df_wifi.withColumn("rounded_longitude", f.bround(f.col("longitude"), 3)).withColumn("rounded_latitude", f.bround(f.col("latitude"), 3))
    df_rounded2.groupBy("rounded_longitude", "rounded_latitude").agg(count("*").alias("entries_count"))
    outputHDFSPath = "/sciezka/do/wynikowej/siatki"
    df_grid.write.mode("append").parquet('hdfs://localhost:8020/user/vagrant/kolos/siatkaWIFI.parquet')
    _logger.info(f'zapisano w hdfs://localhost:8020/user/vagrant/kolos/siatkaWIFI.parquet')
    _logger.info('===== 2.2c =====')
    filtered_df = df_wifi.filter(f.col("ACTIVATED") < "2017-12-31")
    result_df = filtered_df.groupBy("PROVIDER").agg({"BIN": "min", "BIN": "max"})
    result_df = result_df.withColumnRenamed("min(BIN)", "MIN_BIN").withColumnRenamed("max(BIN)", "MAX_BIN")
    result_df.createOrReplaceTempView("min_max_bin_view")
    _logger.info(f'{result_df.show()}')  
    
    df_channels = df_channels.dropDuplicates()
    _logger.info(f'Dropping duplicates... {df_channels.count()} rows left')
    _logger.info('Merging data...')
    df_merge = df_orders\
        .filter(f.col('order_created_month') == 1)\
        .join(df_channels, on='channel_id')\
        .join(df_stores, on='store_id')
    _logger.info(f'data merged : there are {df_merge.count()} records in the merged dataframe')

    _logger.info('===== 2.4 =====')
    _logger.info('Saving merged data...')
    df_merge.write\
        .format('orc')\
        .mode('overwrite')\
        .save('/user/vagrant/kolos/merged_data.orc')
    _logger.info('data has been saved')

    _logger.info('===== 2.5 =====')
    _logger.info(
        df_merge\
            .filter(f.col('order_status') == 'CANCELED')\
            .filter(f.col('channel_type') == 'OWN CHANNEL')\
            .agg(f.max('order_amount'))\
            .take(1)[0]
    )

    _logger.info('===== 2.6 =====')
    grouped_df = df_merge\
        .groupBy('store_segment', 'channel_type', 'order_status', 'order_created_year', 'order_created_month', 'order_created_day')\
        .agg(
            f.sum('order_amount').alias('total_amount'), 
            f.max('order_delivery_cost').alias('max_delivery_cost'),
        )
    take_n = 5
    _logger.info(f'first {take_n} rows of the created dataframe:')
    for row in grouped_df.take(take_n):
        _logger.info(f'{row}')
    
    _logger.info('===== 2.7 =====')
    _logger.info('Creating view...')
    grouped_df.createOrReplaceTempView('grouped_df')
    _logger.info('View created :)')

    _logger.info('===== 2.8 =====')
    _logger.info('result:')
    for row in spark.sql('''
                SELECT COUNT(*) AS count, AVG(total_amount) AS total_amount_avg, store_segment 
                FROM grouped_df 
                GROUP BY store_segment
            ''').collect():
        _logger.info(f'{row}')
    
