'''
@Author: Ayur Ninawe
@Date: 2021-08-24
@Last Modified by: Ayur Ninawe
@Last Modified time: 2021-08-24
@Title : Program to count the number of twitter hashtags.
'''
from logging import error, exception
from pyflink.table import DataTypes, TableEnvironment, EnvironmentSettings
from pyflink.table.descriptors import Schema, OldCsv, FileSystem
from pyflink.table.expressions import e, lit
import os
import random
from Log import logger

from py4j.java_gateway import OutputConsumer

#to operate with Table API
settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
t_env = TableEnvironment.create(settings)

input_file_path = '/home/neo/Programs/FlinkPrograms/input'
output_file_path = '/home/neo/Programs/FlinkPrograms/output'

def createData():
    try:
        hashtags = ['#flink', '#python', '#apache']
        num_tweets = 1000
        with open('input', 'w') as f:
            for tweet in range(num_tweets):
                f.write(random.choice(hashtags)+'\n')

        # write all the data to one file
        t_env.get_config().get_configuration().set_string("parallelism.default", "1")
        #Creates a table source
        t_env.connect(FileSystem().path(input_file_path)) \
            .with_format(OldCsv()
                        .field('word', DataTypes.STRING())) \
            .with_schema(Schema()
                        .field('word', DataTypes.STRING())) \
            .create_temporary_table('mySource')
    except exception as e:
        logger.error(e)

def transformData():
    try:
        #Creates a table sink
        t_env.connect(FileSystem().path(output_file_path)) \
            .with_format(OldCsv()
                        .field_delimiter('\t')
                        .field('word', DataTypes.STRING())
                        .field('count', DataTypes.BIGINT())) \
            .with_schema(Schema()
                        .field('word', DataTypes.STRING())
                        .field('count', DataTypes.BIGINT())) \
            .create_temporary_table('mySink')

        tab = t_env.from_path('mySource')
        tab.group_by(tab.word) \
        .select(tab.word, lit(1).count) \
        .execute_insert('mySink').wait()
    except Exception as e:
        logger.error(e)

if __name__=="__main__":
    createData()
    transformData()