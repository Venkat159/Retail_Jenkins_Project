import pytest
from lib.Utils import get_spark_session
from lib.DataReader import read_customers, read_orders
from lib.DataManipulation import filter_closed_orders, count_orders_state, filter_orders_generic
from lib.ConfigReader import get_app_config

'''
Commented the Code here and placed this code under conftest.py

@pytest.fixture
def spark():
    return get_spark_session("LOCAL")
'''


def test_read_customers_df(spark):
    #spark=get_spark_session("LOCAL") # Instead of this step, we have implemented fixture and passing that to our function
    customers_count = read_customers(spark,"LOCAL").count()
    assert customers_count == 12435

@pytest.mark.skip("It will not be executed since we are skipping")
def test_read_orders_df(spark):
    #spark=get_spark_session("LOCAL") 
    orders_count = read_orders(spark,"LOCAL").count()
    assert orders_count == 68884

@pytest.mark.transformation
def test_filtered_closed_orders(spark):
    #spark = get_spark_session("LOCAL")
    orders_df = read_orders(spark,"LOCAL")
    filtered_count = filter_closed_orders(orders_df).count()
    assert filtered_count == 7556

def test_read_app_config():
    config = get_app_config("LOCAL") 
    assert config["orders.file.path"] == "data/orders.csv" # assert is a pythin function which is used to compare the 2 values If condition satisfy then it will return True

@pytest.mark.slow
def test_count_orders_state(spark, expected_results):
    customers_df = read_customers(spark,"LOCAL")
    Actual_Results = count_orders_state(customers_df)
    assert Actual_Results.collect() == expected_results.collect()

@pytest.mark.latest()
def test_check_closed_count(spark):
    orders_df = read_orders(spark,"LOCAL")
    filtered_count = filter_orders_generic(orders_df,"CLOSED").count()
    assert filtered_count == 7556

@pytest.mark.latest()
def test_check_pendingpayment_count(spark):
    orders_df = read_orders(spark,"LOCAL")
    filtered_count = filter_orders_generic(orders_df,"PENDING_PAYMENT").count()
    assert filtered_count == 15030


@pytest.mark.skip("Instead of HardCoding like this we can go with parameterised approach like below")
def test_check_complete_count(spark):
    orders_df = read_orders(spark,"LOCAL")
    filtered_count = filter_orders_generic(orders_df,"COMPLETE").count()
    assert filtered_count == 22899

@pytest.mark.parametrize(
        "status,count",
        [("CLOSED",7556),
         ("PENDING_PAYMENT",15030),
         ("COMPLETE",22900)]
)
def test_check_count(spark,status,count):
    orders_df = read_orders(spark,"LOCAL")
    filtered_count = filter_orders_generic(orders_df,status).count()
    assert filtered_count == count

'''
1. setup should be done as part of fixture and should not be going in a test case
2. fixture is to write the setup code
3. Ideally we should write our fixture in separate file
4. try writing your fixtures in a file names as conftest.py
5. fileName should be "conftest.py" and frame work knows that it has to read the fixtures from that file only.
6. we no need to tell to spark that my fixtures are kept in this file because frame work knows evrything and it will get the fixtures automatically

'''
'''
**************************   Complete Unit Testing Explanation *******************************
1. Installed pytest frame work using the command "pipenv install pytest".
2. I prepared test cases
3. I created a file with name as test_retail_project.py --> test keywork is mandatory
4. Unit test cases inside the above file should start with test_ (testUnderscore)
5. If we give test_ then that will be automatically identified by pytest for UnitTesting
6. we have created fixture file "conftest.py" --> This is the standard Name and it should be same for fixture file.
7. we need to mention that this is my fixture file and this is the Teardown code and logic. pytest will take case evrything automatically
8.  All we have to do is have to mention the "standard name"s and "tes_"
9. Fixture means getting the resources
10. TearDown means releasing the resources 
11. Code up to yield runs as part of setUp and after yield will be ranned after the unit test case execution.
12. How to run the Unit test 
python -m pytest -v 
In our case we are not using global version of python instead we are using virtual environment so we are gvng our version of python whatver there in the virtual environment and followed by "-m pytest -v "
'''

'''
To LIst down the fixtures use the below command
1. Python -m pytest --fixtures 
2. I want to write one more test case that is my function "count_orders_state" function 
3. This function is doing aggregation correctly or not 
4. I want to test this function "count_orders_state". whether its doing aggregation and count properly or not.
5. we can pass any dataframe to this and check with the expected results.
6. To this function I will pass customers DF only --> count_orders_state(customers_df)
7. If I pass customer data set to the above function I should get the expected result as we have in the file StateAggregate.csv under data folder.
'''
'''
Markers:

For this marker we should create pytest.ini in the main folder

* Markers basically you will label your test cases 
* Lets say you write 100 test cases for different functions 
* 40 of them are to test the transformations, so you can mark them as transformations 
* @pytest.mark.transformation ->  you can put this decorator on top of those test cases and those will be part of transformations.
* command to run transformations python -m pytest -m transformation -v
* If you do the marker and do the transformation like this only that particular marker(test cases) will be executed and rest all will be skipped
* In this way we can execute the test cases of we want from the bunch of test cases.

COMMANDS TO EXECUTE THE TEST CASES:
1. python -m pytest -v   --> To Execute All the Test Cases
2. python -m pytest -m transformation -v  --> To Execute only selected Test Cases/Markers
3. Python -m pytest -m "not transformation" -v  --> To Execute all the Test Cases that which are not transformations
4. python -m pytest --fixtures --> To list down the fixtures including the ones created by you
5. python -m pytest --markers  --> To list down the markers 
6. python -m pytest -m "not slow" -v --> run only now slow test cases
7. python -m pytest -m "slow" -v  --> list down only slow ones
8. python  -m pytest -m "latest" -v
NOTE: The code for above all the command are implemented, we can run the commands directly
'''