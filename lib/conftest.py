import pytest
from lib.Utils import get_spark_session

'''
@pytest.fixture
def spark():
    return get_spark_session("LOCAL")

    # Instead of this code we can use below code where we have implented TearDown Concept (Releasing the resources)
'''


@pytest.fixture
def spark():
    "Creates SparkSession"
    sparkSession = get_spark_session("LOCAL")
    yield sparkSession
    #sparkSession.stop()

@pytest.fixture
def expected_results(spark):
    "Gives Expected Results"
    results_schema = "state string, count int"
    return spark.read \
        .format("csv") \
        .schema(results_schema) \
        .load("data/test_Result/StateAggregate.csv")

'''
1. After our work, How do we realease the recources there should be TearDown pahse also. (Releasing the resources we call it as Tearndown)
2. Thats where instead of return you say yield here 
3. code till yield works as a setUp part and then Unit Test cases run 
4. once unit test cases run all the code after the yield will ru
'''

