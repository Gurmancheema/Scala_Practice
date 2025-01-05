# Learning Spark using Scala
## This repository consists of scala code files and some data files where i practice my data manipulation skills using Scala programming language in Spark framework.

### Concepts learned on 03-01-2025

    1.Filtering Data
        In Spark, filtering data based on specific conditions is a common task. I learned how to filter rows using .where() or .filter(), and how to apply logical operators such as AND and OR to combine multiple conditions.
    2.Finding Distinct Values
        I learned how to find distinct values in a column using .distinct(). This is useful when we want to understand the unique categories or counts in a dataset.
    3.Aggregating Data
        Aggregation is a powerful feature of Spark that allows you to summarize data. I explored how to use the .agg() method to calculate aggregate values like the count of distinct values.

### Concepts learned on 05-01-2025

    1. Infereing schema of the data without explicitly defining it using spark's sampling ratio option
    2. Renaming, adding & dropping columns
        the main method here I learned is to use "withColumnRenamed" & it's parameters.
    3. Changing the datatype of the columns
        changed the string datatype into datetime timestamp data type for date columns
    4. Aggregation statistics functions
        learnt to use groupby(), orderBy() & count() methods along with other descriptive statistical methods like min(), max(), sum(), and avg().
