#PostgreSQL Unit Testing Framework (plpgunit)

Plpgunit started out of curiosity on why a unit testing framework cannot be simple and easy to use. Plpgunit does not require any additional dependencies and is ready to be used on your PostgreSQL Server database.

#Documentation
Please visit the <a href="https://github.com/mixerp/plpgunit/wiki" title= "Plpgsql documentation">wiki page</a>.

# Creating a Plpgunit Unit Test 

A unit test is a plain old function which must: 

* not have any arguments.
* always return "test_result" data type.

#First Thing First
However you could do that,  but there is no need to call each test function manually. The following query automatically invokes all unit tests that have been already created:

	BEGIN TRANSACTION;
	SELECT * FROM unit_tests.begin();
	ROLLBACK TRANSACTION;

Remember, if your test(s) does not contain DML statements, there is no need to BEGIN and ROLLBACK transaction.

#Examples
View documentation for more examples.

## Example #1

	DROP FUNCTION IF EXISTS unit_tests.example1();

	CREATE FUNCTION unit_tests.example1()
	RETURNS test_result
	AS
	$$
	DECLARE message test_result;
	BEGIN
		IF 1 = 1 THEN
			SELECT assert.fail('This failed intentionally.') INTO message;
			RETURN message;
		END IF;

		SELECT assert.ok('End of test.') INTO message;	
		RETURN message;	
	END
	$$
	LANGUAGE plpgsql;

	--BEGIN TRANSACTION;
	SELECT * FROM unit_tests.begin();
	--ROLLBACK TRANSACTION;

**Will Result in**

	Test completed on : 2013-10-18 19:30:01.543 UTC. 
	Total test runtime: 19 ms.

	Total tests run : 1.
	Passed tests    : 0.
	Failed tests    : 1.

	List of failed tests:
	-----------------------------
	unit_tests.example1() --> This failed intentionally.

## Example #2

	DROP FUNCTION IF EXISTS unit_tests.example2()

	CREATE FUNCTION unit_tests.example2()
	RETURNS test_result
	AS
	$$
	DECLARE message test_result;
	DECLARE result boolean;
	DECLARE have integer;
	DECLARE want integer;
	BEGIN
		want := 100;
		SELECT 50 + 49 INTO have;

		SELECT * FROM assert.is_equal(have, want) INTO message, result;

		--Test failed.
		IF result = false THEN
			RETURN message;
		END IF;
		
		--Test passed.
		SELECT assert.ok('End of test.') INTO message;	
		RETURN message;	
	END
	$$
	LANGUAGE plpgsql;

	--BEGIN TRANSACTION;
	SELECT * FROM unit_tests.begin();
	--ROLLBACK TRANSACTION;

**Will Result in**

	Test completed on : 2013-10-18 19:47:11.886 UTC. 
	Total test runtime: 21 ms.

	Total tests run : 2.
	Passed tests    : 0.
	Failed tests    : 2.

	List of failed tests:
	-----------------------------
	unit_tests.example1() --> This failed intentionally.
	unit_tests.example2() --> ASSERT IS_EQUAL FAILED.

	Have -> 99
	Want -> 100

## Example 3

	DROP FUNCTION IF EXISTS unit_tests.example3();

	CREATE FUNCTION unit_tests.example3()
	RETURNS test_result
	AS
	$$
	DECLARE message test_result;
	DECLARE result boolean;
	DECLARE have integer;
	DECLARE dont_want integer;
	BEGIN
		dont_want := 100;
		SELECT 50 + 49 INTO have;

		SELECT * FROM assert.is_not_equal(have, dont_want) INTO message, result;

		--Test failed.
		IF result = false THEN
			RETURN message;
		END IF;
		
		--Test passed.
		SELECT assert.ok('End of test.') INTO message;	
		RETURN message;	
	END
	$$
	LANGUAGE plpgsql;

	--BEGIN TRANSACTION;
	SELECT * FROM unit_tests.begin();
	--ROLLBACK TRANSACTION;

**Will Result in**

	Test completed on : 2013-10-18 19:48:30.578 UTC. 
	Total test runtime: 11 ms.

	Total tests run : 3.
	Passed tests    : 1.
	Failed tests    : 2.

	List of failed tests:
	-----------------------------
	unit_tests.example1() --> This failed intentionally.
	unit_tests.example2() --> ASSERT IS_EQUAL FAILED.

	Have -> 99
	Want -> 100


## Need Contributors for Writing Examples
We need contributors. If you are interested to contribute, let's talk:

<a href="https://www.facebook.com/binod.nirvan/">https://www.facebook.com/binod.nirvan/</a>


Happy testing!
