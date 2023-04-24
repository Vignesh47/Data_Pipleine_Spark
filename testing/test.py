from pyspark.sql import SparkSession
import unittest
import pytest



from src.data_processing import (
    apply_debit_credit_business_classification,
    classify_debit_credit_transactions,
    join_transactionsDf_to_accountsDf,
    normalise_transaction_information,
)


def test_classify_debit_credit_transactions(spark):

    # create input dataframes
    transactionsDf = spark.createDataFrame(
        data=[
            ("1", 1000.00, "123-456-789"),
            ("3", 3000.00, "222222222EUR"),
        ],
        schema=["transaction_id", "amount", "transaction_information"],
    )

    accountsDf = spark.createDataFrame(
        data=[
            ("123456789", "101"),
            ("222222222", "202"),
            ("000000000", "302"),
        ],
        schema=["account_number", "business_line_id"],
    )

    # output dataframe after applying function
    output = classify_debit_credit_transactions(transactionsDf, accountsDf)

    # expected outputs in the target column
    expected_classifications = ["credit", "debit"]

    # assert results are as expected
    assert output.count() == 2
    assert [row.business_line for row in output.collect()] == expected_classifications


def test_normalise_transaction_information(spark):
    data = ["123-456-789", "123456789", "123456789EUR", "TEXT*?WITH.*CHARACTERS"]
    test_df = spark.createDataFrame(data, "string").toDF("transaction_information")

    expected = ["123456789", "123456789", "123456789EUR", "TEXTWITHCHARACTERS"]
    output = normalise_transaction_information(test_df)
    assert [row.transaction_information_cleaned for row in output.collect()] == expected


def test_join_transactionsDf_to_accountsDf(spark):

    data = ["123456789", "222222222EUR"]
    transactionsDf = spark.createDataFrame(data, "string").toDF(
        "transaction_information_cleaned"
    )

    data = [
        "123456789",  # match
        "222222222",  # match
        "000000000",  # no-match
    ]
    accountsDf = spark.createDataFrame(data, "string").toDF("account_number")

    output = join_transactionsDf_to_accountsDf(transactionsDf, accountsDf)

    assert output.count() == 2


def test_apply_debit_credit_business_classification(spark):
    data = [
        "101",  # credit
        "202",  # debit
        "000",  # other
    ]
    df = spark.createDataFrame(data, "string").toDF("business_line_id")
    output = apply_debit_credit_business_classification(df)

    expected = ["credit", "debit", "other"]
    assert [row.business_line for row in output.collect()] == expected
